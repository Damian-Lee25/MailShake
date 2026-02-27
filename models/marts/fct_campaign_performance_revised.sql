{{
    config(
        materialized='table',
        schema='GOLD'
    )
}}

WITH campaigns AS (
    SELECT 
        campaign_id,
        team_id,
        sender_id,
        campaign_title,
        created_at,
        wizard_status,
        sender_email,
        is_archived,
        is_paused
    FROM {{ ref('stg_mailshake_campaigns') }}
),

senders AS (
    SELECT
        sender_id,
        from_name,
        is_verified
    FROM {{ ref('stg_mailshake_senders') }}
),

sent_metrics AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT recipient_id) AS total_recipients,
        COUNT(sent_id) AS emails_sent,
        MIN(action_date) AS first_sent_at,
        MAX(action_date) AS last_sent_at
    FROM {{ ref('stg_mailshake_activity_sent') }}
    GROUP BY campaign_id
),

open_metrics AS (
    SELECT
        campaign_id,
        COUNT(open_id) AS total_opens,
        COUNT(DISTINCT recipient_id) AS unique_openers
    FROM {{ ref('stg_mailshake_activity_opens') }}
    GROUP BY campaign_id
),

click_metrics AS (
    SELECT
        campaign_id,
        COUNT(click_id) AS total_clicks,
        COUNT(DISTINCT recipient_id) AS unique_clickers
    FROM {{ ref('stg_mailshake_activity_clicks') }}
    GROUP BY campaign_id
),

reply_metrics AS (
    SELECT
        campaign_id,
        COUNT(CASE WHEN reply_type = 'reply' THEN reply_id END) AS total_replies,
        COUNT(DISTINCT CASE WHEN reply_type = 'reply' THEN recipient_id END) AS unique_repliers,
        COUNT(CASE WHEN reply_type = 'bounce' THEN reply_id END) AS total_bounces,
        COUNT(CASE WHEN reply_type = 'unsubscribe' THEN reply_id END) AS total_unsubscribes
    FROM {{ ref('stg_mailshake_activity_replies') }}
    GROUP BY campaign_id
)

SELECT
    -- Campaign info
    c.campaign_id,
    c.team_id,
    c.campaign_title,
    c.created_at AS campaign_created_at,
    c.wizard_status,
    c.sender_email,
    se.from_name AS sender_name,        -- ✅ Changed 's' to 'se'
    se.is_verified AS sender_verified,  -- ✅ Changed 's' to 'se'
    c.is_archived,
    c.is_paused,
    
    -- Volume metrics
    COALESCE(sm.total_recipients, 0) AS total_recipients,      -- ✅ Changed 's' to 'sm'
    COALESCE(sm.emails_sent, 0) AS emails_sent,                -- ✅ Changed 's' to 'sm'
    COALESCE(o.total_opens, 0) AS total_opens,
    COALESCE(o.unique_openers, 0) AS unique_openers,
    COALESCE(cl.total_clicks, 0) AS total_clicks,
    COALESCE(cl.unique_clickers, 0) AS unique_clickers,
    COALESCE(r.total_replies, 0) AS total_replies,
    COALESCE(r.unique_repliers, 0) AS unique_repliers,
    COALESCE(r.total_bounces, 0) AS total_bounces,
    COALESCE(r.total_unsubscribes, 0) AS total_unsubscribes,
    
    -- Timing
    sm.first_sent_at,        -- ✅ Changed 's' to 'sm'
    sm.last_sent_at,         -- ✅ Changed 's' to 'sm'
    
    -- Core rates (0-1 scale)
    SAFE_DIVIDE(o.unique_openers, sm.emails_sent) AS open_rate,           -- ✅ Changed 's' to 'sm'
    SAFE_DIVIDE(cl.unique_clickers, sm.emails_sent) AS click_rate,        -- ✅ Changed 's' to 'sm'
    SAFE_DIVIDE(r.unique_repliers, sm.emails_sent) AS reply_rate,         -- ✅ Changed 's' to 'sm'
    SAFE_DIVIDE(cl.unique_clickers, o.unique_openers) AS click_through_rate,
    SAFE_DIVIDE(r.total_bounces, sm.emails_sent) AS bounce_rate,          -- ✅ Changed 's' to 'sm'
    SAFE_DIVIDE(r.total_unsubscribes, sm.emails_sent) AS unsubscribe_rate,-- ✅ Changed 's' to 'sm'
    
    -- Metadata
    CURRENT_TIMESTAMP() AS calculated_at

FROM campaigns c
LEFT JOIN senders se ON c.sender_id = se.sender_id
LEFT JOIN sent_metrics sm ON c.campaign_id = sm.campaign_id  -- ✅ Changed alias to 'sm'
LEFT JOIN open_metrics o ON c.campaign_id = o.campaign_id
LEFT JOIN click_metrics cl ON c.campaign_id = cl.campaign_id
LEFT JOIN reply_metrics r ON c.campaign_id = r.campaign_id