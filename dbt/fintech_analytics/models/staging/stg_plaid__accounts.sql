with source as (
    select * from {{ source('plaid', 'raw_accounts') }}
),

renamed as (
    select
        JSON_VALUE(raw_json, '$.account_id')                as account_id,
        institution_id,
        trim(JSON_VALUE(raw_json, '$.name'))                as name,
        trim(JSON_VALUE(raw_json, '$.official_name'))       as official_name,
        JSON_VALUE(raw_json, '$.mask')                      as mask,
        lower(JSON_VALUE(raw_json, '$.type'))               as type,
        lower(JSON_VALUE(raw_json, '$.subtype'))            as subtype,
        ingestion_date,
        _ingested_at,
        _source_file
    from source
)

select * from renamed
