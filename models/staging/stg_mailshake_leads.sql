{{
    config(
        materialized='view',
        schema='SILVER'
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ source('bronze', 'leads') }}
    
),

parsed AS (

    SELECT
        -- ingestion metadata
        team_id,
        extracted_at,
        source_endpoint,

        -- primary identifier
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS lead_id,

        -- lead attributes
        JSON_VALUE(payload, '$.status') AS lead_status,
        JSON_VALUE(payload, '$.annotation') AS annotation,
        JSON_VALUE(payload, '$.object') AS object_type,

        -- timestamps
        CAST(JSON_VALUE(payload, '$.created') AS TIMESTAMP) AS created_at,
        CAST(JSON_VALUE(payload, '$.openedDate') AS TIMESTAMP) AS opened_at,
        CAST(JSON_VALUE(payload, '$.lastStatusChangeDate') AS TIMESTAMP) AS last_status_change_at,

        -- campaign context
        CAST(JSON_VALUE(payload, '$.campaign.id') AS INT64) AS campaign_id,
        JSON_VALUE(payload, '$.campaign.title') AS campaign_title,
        JSON_VALUE(payload, '$.campaign.object') AS campaign_object,
        JSON_VALUE(payload, '$.campaign.wizardStatus') AS campaign_wizard_status,

        -- recipient context
        CAST(JSON_VALUE(payload, '$.recipient.id') AS INT64) AS recipient_id,
        JSON_VALUE(payload, '$.recipient.emailAddress') AS recipient_email,
        JSON_VALUE(payload, '$.recipient.first') AS recipient_first_name,
        JSON_VALUE(payload, '$.recipient.last') AS recipient_last_name,
        JSON_VALUE(payload, '$.recipient.fullName') AS recipient_full_name,
        CAST(JSON_VALUE(payload, '$.recipient.created') AS TIMESTAMP) AS recipient_created_at,
        CAST(JSON_VALUE(payload, '$.recipient.isPaused') AS BOOL) AS recipient_is_paused,

        -- recipient custom fields
        JSON_VALUE(payload, '$.recipient.fields.account') AS account,
        JSON_VALUE(payload, '$.recipient.fields.position') AS position,
        JSON_VALUE(payload, '$.recipient.fields.link') AS job_link,
        JSON_VALUE(payload, '$.recipient.fields.phoneNumber') AS phone_number,
        JSON_VALUE(payload, '$.recipient.fields.linkedInUrl') AS linkedin_url,
        JSON_VALUE(payload, '$.recipient.fields.facebookUrl') AS facebook_url,
        JSON_VALUE(payload, '$.recipient.fields.instagramID') AS instagram_id,
        JSON_VALUE(payload, '$.recipient.fields.twitterID') AS twitter_id

    FROM source
    WHERE CAST(JSON_VALUE(payload, '$.isDuplicate') AS BOOL) = FALSE

),

deduped AS (
    SELECT *
    FROM parsed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lead_id 
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT *
FROM deduped
