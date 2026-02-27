

  create or replace view `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_recipients`
  OPTIONS()
  as 

WITH source AS (
    SELECT * FROM `mailshake-analysis-486717`.`BRONZE`.`recipients`
),

parsed AS (
    SELECT
        -- Ingestion metadata
        team_id,
        CAST(campaign_id AS INT64) AS campaign_id,  -- ✅ CHANGED - Cast to INT64
        extracted_at,
        source_endpoint,

        -- Primary identifier
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS recipient_id,

        -- Recipient identity
        JSON_VALUE(payload, '$.emailAddress') AS email_address,
        JSON_VALUE(payload, '$.first') AS first_name,
        JSON_VALUE(payload, '$.last') AS last_name,
        JSON_VALUE(payload, '$.fullName') AS full_name,

        -- Recipient attributes
        CAST(JSON_VALUE(payload, '$.created') AS TIMESTAMP) AS created_at,
        CAST(JSON_VALUE(payload, '$.isPaused') AS BOOL) AS is_paused,
        JSON_VALUE(payload, '$.object') AS object_type,

        -- Custom fields (Nested JSON navigation)
        JSON_VALUE(payload, '$.fields.account') AS account,
        JSON_VALUE(payload, '$.fields.position') AS position,
        JSON_VALUE(payload, '$.fields.phoneNumber') AS phone_number,
        JSON_VALUE(payload, '$.fields.link') AS link,
        JSON_VALUE(payload, '$.fields.linkedInUrl') AS linkedin_url,
        JSON_VALUE(payload, '$.fields.facebookUrl') AS facebook_url,
        JSON_VALUE(payload, '$.fields.instagramID') AS instagram_id,
        JSON_VALUE(payload, '$.fields.twitterID') AS twitter_id

    FROM source
)

-- Deduplication
SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY recipient_id 
    ORDER BY extracted_at DESC
) = 1;

