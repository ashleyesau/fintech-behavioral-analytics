with transactions as (
    select * from {{ ref('int_transactions_enriched') }}
),

cashflow as (
    select * from {{ ref('int_account_monthly_cashflow') }}
),

-- aggregate merchant spend before applying window function
monthly_merchant_spend as (
    select
        account_id,
        date_trunc(transaction_date, month)     as month,
        merchant_name,
        sum(amount)                             as merchant_spend
    from transactions
    where is_debit = true
        and merchant_name is not null
        and merchant_name != ''
    group by
        account_id,
        date_trunc(transaction_date, month),
        merchant_name
),

-- rank merchants after aggregation to avoid ungrouped column in window
monthly_top_merchants as (
    select
        account_id,
        month,
        merchant_name,
        merchant_spend,
        row_number() over (
            partition by account_id, month
            order by merchant_spend desc
        )                                       as merchant_rank
    from monthly_merchant_spend
),

top3_merchants_pivoted as (
    select
        account_id,
        month,
        max(case when merchant_rank = 1 then merchant_name end) as top_merchant_1,
        max(case when merchant_rank = 2 then merchant_name end) as top_merchant_2,
        max(case when merchant_rank = 3 then merchant_name end) as top_merchant_3
    from monthly_top_merchants
    where merchant_rank <= 3
    group by account_id, month
),

-- recurring vs discretionary split per account per month
monthly_spend_split as (
    select
        account_id,
        date_trunc(transaction_date, month)                         as month,
        sum(case when is_recurring then amount else 0 end)          as recurring_spend,
        sum(case when not is_recurring then amount else 0 end)      as discretionary_spend,
        sum(amount)                                                 as total_spend,
        count(distinct case when merchant_name is not null
            and merchant_name != '' then merchant_name end)         as unique_merchant_count
    from transactions
    where is_debit = true
    group by account_id, date_trunc(transaction_date, month)
),

-- spend spike: current month vs account historical mean + 2 stddev
monthly_spend_stats as (
    select
        account_id,
        month,
        total_spend,
        avg(cast(total_spend as float64)) over (
            partition by account_id
        )                                       as avg_monthly_spend,
        stddev(cast(total_spend as float64)) over (
            partition by account_id
        )                                       as stddev_monthly_spend
    from monthly_spend_split
),

final as (
    select
        c.account_id,
        c.institution_id,
        c.month,
        coalesce(sp.recurring_spend, 0)         as recurring_spend,
        coalesce(sp.discretionary_spend, 0)     as discretionary_spend,
        coalesce(sp.total_spend, 0)             as total_spend,
        round(safe_divide(
            coalesce(sp.recurring_spend, 0),
            nullif(coalesce(sp.total_spend, 0), 0)
        ), 4)                                   as recurring_spend_pct,
        coalesce(sp.unique_merchant_count, 0)   as unique_merchant_count,
        tm.top_merchant_1,
        tm.top_merchant_2,
        tm.top_merchant_3,
        case
            when ss.stddev_monthly_spend > 0
                and sp.total_spend > ss.avg_monthly_spend + (2 * ss.stddev_monthly_spend)
            then true
            else false
        end                                     as spend_spike_flag
    from cashflow c
    left join monthly_spend_split sp
        on c.account_id = sp.account_id
        and c.month = sp.month
    left join top3_merchants_pivoted tm
        on c.account_id = tm.account_id
        and c.month = tm.month
    left join monthly_spend_stats ss
        on c.account_id = ss.account_id
        and c.month = ss.month
)

select * from final
