with transactions as (
    select * from {{ ref('int_transactions_enriched') }}
),

date_bounds as (
    select
        date_trunc(min(transaction_date), month) as min_month,
        date_trunc(max(transaction_date), month) as max_month
    from transactions
),

month_spine as (
    select month
    from date_bounds,
    unnest(generate_date_array(min_month, max_month, interval 1 month)) as month
),

accounts as (
    select distinct account_id, institution_id
    from transactions
),

account_month_spine as (
    select
        a.account_id,
        a.institution_id,
        s.month
    from accounts a
    cross join month_spine s
),

monthly_transactions as (
    select
        account_id,
        date_trunc(transaction_date, month)                         as month,
        sum(case when is_debit then amount else 0 end)              as total_debits,
        sum(case when not is_debit then abs(amount) else 0 end)     as total_credits,
        count(*)                                                    as transaction_count,
        countif(is_debit)                                           as debit_count,
        countif(not is_debit)                                       as credit_count
    from transactions
    group by
        account_id,
        date_trunc(transaction_date, month)
),

joined as (
    select
        spine.account_id,
        spine.institution_id,
        spine.month,
        coalesce(mt.total_debits, 0)                                as total_debits,
        coalesce(mt.total_credits, 0)                               as total_credits,
        coalesce(mt.total_credits, 0) - coalesce(mt.total_debits, 0) as net_cashflow,
        coalesce(mt.transaction_count, 0)                           as transaction_count,
        coalesce(mt.debit_count, 0)                                 as debit_count,
        coalesce(mt.credit_count, 0)                                as credit_count
    from account_month_spine spine
    left join monthly_transactions mt
        on spine.account_id = mt.account_id
        and spine.month = mt.month
)

select * from joined
