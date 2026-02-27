

  create or replace view `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_campaigns`
  OPTIONS()
  as 

WITH source AS (
    SELECT * FROM `mailshake-analysis-486717`.`BRONZE`.`campaigns`
),

parsed AS (
    SELECT
        -- Bronze metadata
        team_id,
        extracted_at,
        source_endpoint,
        
        -- Campaign attributes
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS campaign_id,
        CAST(JSON_VALUE(payload, '$.created') AS TIMESTAMP) AS created_at,
        JSON_VALUE(payload, '$.title') AS campaign_title,
        JSON_VALUE(payload, '$.object') AS object_type,
        JSON_VALUE(payload, '$.url') AS campaign_url,
        JSON_VALUE(payload, '$.wizardStatus') AS wizard_status,
        CAST(JSON_VALUE(payload, '$.isArchived') AS BOOL) AS is_archived,
        CAST(JSON_VALUE(payload, '$.isPaused') AS BOOL) AS is_paused,
        
        -- Sender info (Nested JSON)
        JSON_VALUE(payload, '$.sender.id') AS sender_id,
        JSON_VALUE(payload, '$.sender.emailAddress') AS sender_email,
        JSON_VALUE(payload, '$.sender.fromName') AS sender_from_name,
        CAST(JSON_VALUE(payload, '$.sender.created') AS TIMESTAMP) AS sender_created_at,
        
        -- Messages array
        JSON_QUERY(payload, '$.messages') AS messages_json
        
    FROM source
    -- Deduplication logic happens here
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campaign_id 
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT * FROM parsed;

