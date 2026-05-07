with cashflow as (
    select * from {{ ref('int_account_monthly_cashflow') }}
),

with_trend as (
    select
        account_id,
        institution_id,
        month,
        total_debits,
        total_credits,
        net_cashflow,
        transaction_count,

        -- 3-month average net cashflow (current month + 2 prior months)
        avg(net_cashflow) over (
            partition by account_id
            order by month
            rows between 2 preceding and current row
        ) as avg_net_cashflow_3m,

        -- savings rate proxy: proportion of credits retained after debits
        safe_divide(net_cashflow, nullif(total_credits, 0)) as savings_rate_proxy,

        -- lag for trend comparison
        lag(net_cashflow, 1) over (
            partition by account_id order by month
        ) as prior_month_net_cashflow

    from cashflow
),

final as (
    select
        account_id,
        institution_id,
        month,
        total_debits,
        total_credits,
        net_cashflow,
        transaction_count,
        round(avg_net_cashflow_3m, 2)           as avg_net_cashflow_3m,
        round(savings_rate_proxy, 4)            as savings_rate_proxy,
        case
            when prior_month_net_cashflow is null
                then 'STABLE'
            when net_cashflow > avg_net_cashflow_3m * 1.1
                then 'IMPROVING'
            when net_cashflow < avg_net_cashflow_3m * 0.9
                then 'DECLINING'
            else 'STABLE'
        end                                     as cashflow_trend_3m
    from with_trend
)

select * from final
