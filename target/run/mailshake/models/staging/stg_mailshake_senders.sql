

  create or replace view `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_senders`
  OPTIONS()
  as 

WITH source AS (
    SELECT * 
    FROM `mailshake-analysis-486717`.`BRONZE`.`senders`
),

parsed AS (
    SELECT
        -- Ingestion metadata
        team_id,
        extracted_at,
        source_endpoint,

        -- Primary identifier
        JSON_VALUE(payload, '$.id') AS sender_id,

        -- Sender attributes
        JSON_VALUE(payload, '$.emailAddress') AS email_address,  -- ✅ CHANGED from sender_email
        JSON_VALUE(payload, '$.fromName') AS from_name,  -- ✅ CHANGED from sender_name (to match staging.yml)
        CAST(JSON_VALUE(payload, '$.created') AS TIMESTAMP) AS created_at,
        CAST(JSON_VALUE(payload, '$.isVerified') AS BOOL) AS is_verified,  -- ✅ ADDED (from staging.yml)
        JSON_VALUE(payload, '$.object') AS object_type

    FROM source
)

-- Deduplication
SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sender_id 
    ORDER BY extracted_at DESC
) = 1;

