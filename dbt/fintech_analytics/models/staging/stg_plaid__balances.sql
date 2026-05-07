with source as (
    select * from {{ source('plaid', 'raw_balances') }}
),

renamed as (
    select
        JSON_VALUE(raw_json, '$.account_id')                            as account_id,
        institution_id,
        SAFE_CAST(JSON_VALUE(raw_json, '$.balance_current') AS NUMERIC) as balance_current,
        SAFE_CAST(JSON_VALUE(raw_json, '$.balance_available') AS NUMERIC) as balance_available,
        SAFE_CAST(JSON_VALUE(raw_json, '$.balance_limit') AS NUMERIC)   as balance_limit,
        JSON_VALUE(raw_json, '$.iso_currency_code')                     as iso_currency_code,
        ingestion_date                                                  as snapshot_date,
        _ingested_at,
        _source_file
    from source
)

select * from renamed
