with transactions as (
    select * from {{ ref('stg_plaid__transactions') }}
),

enriched as (
    select
        -- pass through all staging columns
        transaction_id,
        account_id,
        institution_id,
        merchant_name,
        transaction_name,
        transaction_type,
        payment_channel,
        merchant_category_id,
        amount,
        is_debit,
        iso_currency_code,
        transaction_date,
        authorized_date,
        ingestion_date,
        _ingested_at,
        _source_file,

        -- derived: direction
        case
            when is_debit then 'debit'
            else 'credit'
        end as transaction_direction,

        -- derived: normalised category
        case
            when lower(merchant_name) like '%payroll%'
                or lower(transaction_name) like '%payroll%'
                or lower(transaction_name) like '%direct deposit%'
                or lower(transaction_name) like '%salary%'
                then 'income'
            when lower(merchant_name) like '%venmo%'
                or lower(merchant_name) like '%zelle%'
                or lower(merchant_name) like '%paypal%'
                or lower(transaction_name) like '%transfer%'
                or lower(transaction_type) = 'special'
                then 'transfer'
            when lower(merchant_name) like '%walmart%'
                or lower(merchant_name) like '%kroger%'
                or lower(merchant_name) like '%whole foods%'
                or lower(merchant_name) like '%trader joe%'
                or lower(merchant_name) like '%safeway%'
                or lower(merchant_name) like '%publix%'
                or lower(merchant_name) like '%aldi%'
                or lower(merchant_name) like '%costco%'
                then 'groceries'
            when lower(merchant_name) like '%restaurant%'
                or lower(merchant_name) like '%mcdonald%'
                or lower(merchant_name) like '%starbucks%'
                or lower(merchant_name) like '%chipotle%'
                or lower(merchant_name) like '%doordash%'
                or lower(merchant_name) like '%uber eats%'
                or lower(merchant_name) like '%grubhub%'
                or lower(merchant_name) like '%chick-fil%'
                or lower(merchant_name) like '%taco bell%'
                or lower(merchant_name) like '%subway%'
                then 'dining'
            when lower(merchant_name) like '%uber%'
                or lower(merchant_name) like '%lyft%'
                or lower(merchant_name) like '%shell%'
                or lower(merchant_name) like '%chevron%'
                or lower(merchant_name) like '%exxon%'
                or lower(merchant_name) like '%bp%'
                or lower(transaction_name) like '%transit%'
                or lower(transaction_name) like '%parking%'
                then 'transport'
            when lower(merchant_name) like '%electric%'
                or lower(merchant_name) like '%water%'
                or lower(merchant_name) like '%utility%'
                or lower(merchant_name) like '%comcast%'
                or lower(merchant_name) like '%at&t%'
                or lower(merchant_name) like '%verizon%'
                or lower(merchant_name) like '%t-mobile%'
                or lower(merchant_name) like '%spectrum%'
                then 'utilities'
            when lower(merchant_name) like '%netflix%'
                or lower(merchant_name) like '%spotify%'
                or lower(merchant_name) like '%hulu%'
                or lower(merchant_name) like '%disney%'
                or lower(merchant_name) like '%apple%'
                or lower(merchant_name) like '%google%'
                or lower(merchant_name) like '%amazon prime%'
                or lower(merchant_name) like '%youtube%'
                then 'subscriptions'
            else 'other'
        end as merchant_category_normalised,

        -- derived: recurring flag
        case
            when lower(merchant_name) like '%netflix%'
                or lower(merchant_name) like '%spotify%'
                or lower(merchant_name) like '%hulu%'
                or lower(merchant_name) like '%disney%'
                or lower(merchant_name) like '%apple%'
                or lower(merchant_name) like '%google%'
                or lower(merchant_name) like '%amazon prime%'
                or lower(merchant_name) like '%youtube%'
                or lower(merchant_name) like '%comcast%'
                or lower(merchant_name) like '%at&t%'
                or lower(merchant_name) like '%verizon%'
                or lower(merchant_name) like '%t-mobile%'
                or lower(merchant_name) like '%spectrum%'
                or lower(merchant_name) like '%electric%'
                or lower(merchant_name) like '%water%'
                or lower(merchant_name) like '%utility%'
                or lower(merchant_name) like '%gym%'
                or lower(merchant_name) like '%planet fitness%'
                or lower(merchant_name) like '%insurance%'
                or lower(transaction_name) like '%rent%'
                or lower(transaction_name) like '%mortgage%'
                then true
            else false
        end as is_recurring

    from transactions
)

select * from enriched
