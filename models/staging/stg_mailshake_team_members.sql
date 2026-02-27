{{
    config(
        materialized='view',
        schema='SILVER'
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ source('bronze', 'team_members') }}
),

parsed AS (
    SELECT
        -- Ingestion metadata
        team_id,  -- ✅ FIXED - Use the bronze table's team_id column
        extracted_at,
        source_endpoint,

        -- Primary identifier
        CAST(JSON_VALUE(payload, '$.id') AS INT64) AS user_id,  -- ✅ CHANGED from member_id

        -- Personal info
        JSON_VALUE(payload, '$.emailAddress') AS email_address,
        JSON_VALUE(payload, '$.first') AS first_name,
        JSON_VALUE(payload, '$.last') AS last_name,
        JSON_VALUE(payload, '$.fullName') AS full_name,

        -- Status / role flags
        CAST(JSON_VALUE(payload, '$.isDisabled') AS BOOL) AS is_disabled,
        CAST(JSON_VALUE(payload, '$.isTeamAdmin') AS BOOL) AS is_team_admin,
        
        -- ✅ ADDED - Derive role from is_team_admin
        CASE 
            WHEN CAST(JSON_VALUE(payload, '$.isTeamAdmin') AS BOOL) = TRUE THEN 'admin'
            ELSE 'member'
        END AS role,
        
        JSON_VALUE(payload, '$.object') AS object_type

    FROM source
)

-- Deduplication
SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id 
    ORDER BY extracted_at DESC
) = 1