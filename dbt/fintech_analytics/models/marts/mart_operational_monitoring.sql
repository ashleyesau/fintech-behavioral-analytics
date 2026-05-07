with transactions as (
    select * from {{ ref('stg_plaid__transactions') }}
),

balances as (
    select * from {{ ref('stg_plaid__balances') }}
),

-- transaction counts per institution per ingestion date
daily_transaction_counts as (
    select
        institution_id,
        ingestion_date,
        count(*)                                        as records_loaded,
        count(distinct transaction_id)                  as distinct_transaction_ids
    from transactions
    group by institution_id, ingestion_date
),

-- expected record count: average daily load per institution
expected_counts as (
    select
        institution_id,
        avg(cast(records_loaded as float64))            as avg_daily_records
    from daily_transaction_counts
    group by institution_id
),

-- duplicate detection: records_loaded vs distinct transaction_ids
-- a gap here flags duplicate transactions within the partition
with_duplicates as (
    select
        d.institution_id,
        d.ingestion_date,
        d.records_loaded,
        d.distinct_transaction_ids,
        d.records_loaded - d.distinct_transaction_ids   as duplicate_transaction_count,
        e.avg_daily_records                             as expected_records
    from daily_transaction_counts d
    left join expected_counts e on d.institution_id = e.institution_id
),

-- balance snapshot completeness: accounts with balances per ingestion date
daily_balance_counts as (
    select
        institution_id,
        snapshot_date                                   as ingestion_date,
        count(distinct account_id)                      as accounts_with_balances
    from balances
    group by institution_id, snapshot_date
),

final as (
    select
        t.institution_id,
        t.ingestion_date,
        t.records_loaded,
        t.distinct_transaction_ids,
        t.duplicate_transaction_count,
        round(t.expected_records, 1)                    as expected_records,
        round(
            safe_divide(
                cast(t.records_loaded as float64),
                nullif(t.expected_records, 0)
            ) * 100,
            2
        )                                               as completeness_pct,
        coalesce(b.accounts_with_balances, 0)           as accounts_with_balances,
        case
            when t.duplicate_transaction_count > 0      then true
            else false
        end                                             as has_duplicates,
        case
            when date_diff(current_date(), t.ingestion_date, day) = 0  then 'FRESH'
            when date_diff(current_date(), t.ingestion_date, day) <= 7  then 'STALE'
            else 'MISSING'
        end                                             as freshness_status
    from with_duplicates t
    left join daily_balance_counts b
        on t.institution_id = b.institution_id
        and t.ingestion_date = b.ingestion_date
)

select * from final
