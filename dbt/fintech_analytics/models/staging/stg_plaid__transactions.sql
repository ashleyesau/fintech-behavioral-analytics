with source as (

    select * from {{ source('plaid', 'raw_transactions') }}

),

staged as (

    select
        -- identifiers
        transaction_id,
        account_id,
        institution_id,

        -- transaction details
        TRIM(LOWER(merchant_name))                              as merchant_name,
        JSON_VALUE(raw_json, '$.name')                          as transaction_name,
        LOWER(transaction_type)                                 as transaction_type,
        payment_channel,
        merchant_category_id,

        -- amounts and direction
        CAST(amount as NUMERIC)                                 as amount,
        CAST(amount as NUMERIC) > 0                             as is_debit,
        JSON_VALUE(raw_json, '$.iso_currency_code')             as iso_currency_code,

        -- dates
        CAST(date as DATE)                                      as transaction_date,
        CAST(JSON_VALUE(raw_json, '$.authorized_date') as DATE) as authorized_date,

        -- metadata
        ingestion_date,
        _ingested_at,
        _source_file

    from source

    where coalesce(pending, false) = false

)

select * from staged
