

WITH source AS (
    SELECT *
    FROM `mailshake-analysis-486717`.`BRONZE`.`activity_replies`
),

parsed AS (
    SELECT
        -- Ingestion metadata
        team_id,
        CAST(campaign_id AS INT64) AS campaign_id,  -- ✅ From BRONZE table column
        CAST(message_id AS INT64) AS message_id,     -- ✅ From BRONZE table column
        extracted_at,
        source_endpoint,

        -- Primary identifier
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS reply_id,

        -- Event metadata
        CAST(JSON_VALUE(payload, '$.actionDate') AS TIMESTAMP) AS action_date,
        JSON_VALUE(payload, '$.object') AS object_type,
        JSON_VALUE(payload, '$.type') AS reply_type,
        JSON_VALUE(payload, '$.subject') AS subject,

        -- External IDs
        JSON_VALUE(payload, '$.externalConversationID') AS external_conversation_id,
        JSON_VALUE(payload, '$.externalID') AS external_id,
        JSON_VALUE(payload, '$.externalRawMessageID') AS external_raw_message_id,

        -- Campaign info (from nested payload - backup if needed)
        CAST(JSON_VALUE(payload, '$.campaign.id') AS INT64) AS payload_campaign_id,
        JSON_VALUE(payload, '$.campaign.title') AS campaign_title,
        JSON_VALUE(payload, '$.campaign.object') AS campaign_object,
        JSON_VALUE(payload, '$.campaign.wizardStatus') AS campaign_wizard_status,

        -- Parent sent message info
        CAST(JSON_VALUE(payload, '$.parent.id') AS INT64) AS parent_sent_id,
        JSON_VALUE(payload, '$.parent.type') AS parent_type,
        JSON_VALUE(payload, '$.parent.object') AS parent_object,
        CAST(JSON_VALUE(payload, '$.parent.message.id') AS INT64) AS parent_message_id,
        JSON_VALUE(payload, '$.parent.message.type') AS parent_message_type,
        CAST(JSON_VALUE(payload, '$.parent.message.replyToID') AS INT64) AS parent_reply_to_id,
        JSON_VALUE(payload, '$.parent.message.subject') AS parent_subject,

        -- From address info
        JSON_VALUE(payload, '$.from.address') AS from_address,
        JSON_VALUE(payload, '$.from.first') AS from_first_name,
        JSON_VALUE(payload, '$.from.last') AS from_last_name,
        JSON_VALUE(payload, '$.from.fullName') AS from_full_name,

        -- Recipient info
        CAST(JSON_VALUE(payload, '$.recipient.id') AS INT64) AS recipient_id,
        CAST(JSON_VALUE(payload, '$.recipient.contactID') AS INT64) AS recipient_contact_id,
        JSON_VALUE(payload, '$.recipient.emailAddress') AS recipient_email,
        JSON_VALUE(payload, '$.recipient.first') AS recipient_first_name,
        JSON_VALUE(payload, '$.recipient.last') AS recipient_last_name,
        JSON_VALUE(payload, '$.recipient.fullName') AS recipient_full_name,
        CAST(JSON_VALUE(payload, '$.recipient.created') AS TIMESTAMP) AS recipient_created_at,
        CAST(JSON_VALUE(payload, '$.recipient.isPaused') AS BOOL) AS recipient_is_paused,

        -- Recipient custom fields
        JSON_VALUE(payload, '$.recipient.fields.account') AS account,
        JSON_VALUE(payload, '$.recipient.fields.position') AS position,
        JSON_VALUE(payload, '$.recipient.fields.link') AS job_link,
        JSON_VALUE(payload, '$.recipient.fields.phoneNumber') AS phone_number,
        JSON_VALUE(payload, '$.recipient.fields.linkedInUrl') AS linkedin_url,
        JSON_VALUE(payload, '$.recipient.fields.facebookUrl') AS facebook_url,
        JSON_VALUE(payload, '$.recipient.fields.instagramID') AS instagram_id,
        JSON_VALUE(payload, '$.recipient.fields.twitterID') AS twitter_id,

        -- Message body
        JSON_VALUE(payload, '$.body') AS html_body,
        JSON_VALUE(payload, '$.plainTextBody') AS plain_text_body,
        JSON_VALUE(payload, '$.rawBody') AS raw_body

    FROM source
)

-- Deduplication
SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY reply_id
    ORDER BY extracted_at DESC
) = 1