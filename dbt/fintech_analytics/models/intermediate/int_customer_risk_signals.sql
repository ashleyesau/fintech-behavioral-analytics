{{ config(materialized='table') }}

with cashflow as (
    select * from {{ ref('int_account_monthly_cashflow') }}
),

transactions as (
    select * from {{ ref('int_transactions_enriched') }}
),

-- gaps and islands: max consecutive negative cashflow months per account
monthly_flags as (
    select
        account_id,
        month,
        net_cashflow,
        case when net_cashflow < 0 then 1 else 0 end               as is_negative,
        row_number() over (partition by account_id order by month)  as rn
    from cashflow
),

negative_months as (
    select
        account_id,
        month,
        rn - row_number() over (partition by account_id order by month) as island_id
    from monthly_flags
    where is_negative = 1
),

streak_lengths as (
    select
        account_id,
        island_id,
        count(*) as streak_length
    from negative_months
    group by account_id, island_id
),

max_streaks as (
    select
        account_id,
        max(streak_length) as max_negative_cashflow_streak
    from streak_lengths
    group by account_id
),

-- merchant concentration: top 3 merchants as pct of total debit spend
merchant_spend_by_name as (
    select
        account_id,
        merchant_name,
        cast(sum(amount) as float64) as spend_amount
    from transactions
    where is_debit = true
        and merchant_name is not null
        and merchant_name != ''
    group by account_id, merchant_name
),

merchant_ranked as (
    select
        account_id,
        merchant_name,
        spend_amount,
        row_number() over (partition by account_id order by spend_amount desc) as merchant_rank
    from merchant_spend_by_name
),

total_spend as (
    select
        account_id,
        sum(spend_amount) as total_spend
    from merchant_spend_by_name
    group by account_id
),

top3_spend as (
    select
        account_id,
        sum(spend_amount) as top3_spend
    from merchant_ranked
    where merchant_rank <= 3
    group by account_id
),

concentration as (
    select
        t.account_id,
        safe_divide(top3.top3_spend, t.total_spend) as merchant_concentration_ratio
    from total_spend t
    left join top3_spend top3 on t.account_id = top3.account_id
),

-- cashflow volatility: stddev of monthly net cashflow per account
volatility as (
    select
        account_id,
        stddev(cast(net_cashflow as float64)) as cashflow_volatility
    from cashflow
    group by account_id
),

-- account base for spine
account_base as (
    select distinct account_id, institution_id
    from cashflow
),

final as (
    select
        a.account_id,
        a.institution_id,
        coalesce(ms.max_negative_cashflow_streak, 0)        as max_negative_cashflow_streak,
        coalesce(c.merchant_concentration_ratio, 0)         as merchant_concentration_ratio,
        coalesce(v.cashflow_volatility, 0)                  as cashflow_volatility,
        case
            when coalesce(c.merchant_concentration_ratio, 0) >= 0.6
                and coalesce(ms.max_negative_cashflow_streak, 0) >= 2
            then true
            else false
        end                                                  as combined_risk_flag
    from account_base a
    left join max_streaks ms  on a.account_id = ms.account_id
    left join concentration c on a.account_id = c.account_id
    left join volatility v    on a.account_id = v.account_id
)

select * from final
