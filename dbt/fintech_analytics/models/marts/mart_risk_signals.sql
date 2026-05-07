with risk_signals as (
    select * from {{ ref('int_customer_risk_signals') }}
),

balances as (
    select * from {{ ref('stg_plaid__balances') }}
),

latest_balances as (
    select
        account_id,
        balance_current,
        balance_available,
        balance_limit,
        snapshot_date,
        row_number() over (partition by account_id order by snapshot_date desc) as rn
    from balances
),

most_recent_balance as (
    select
        account_id,
        balance_current,
        balance_available,
        balance_limit,
        snapshot_date as balance_snapshot_date
    from latest_balances
    where rn = 1
),

final as (
    select
        r.account_id,
        r.institution_id,
        r.max_negative_cashflow_streak,
        r.merchant_concentration_ratio,
        r.cashflow_volatility,
        r.combined_risk_flag,
        case
            when r.combined_risk_flag = true
                then 'HIGH'
            when r.merchant_concentration_ratio >= 0.6
                or r.max_negative_cashflow_streak >= 2
                then 'MEDIUM'
            else 'LOW'
        end                                     as risk_tier,
        b.balance_current,
        b.balance_available,
        b.balance_limit,
        b.balance_snapshot_date
    from risk_signals r
    left join most_recent_balance b on r.account_id = b.account_id
)

select * from final
