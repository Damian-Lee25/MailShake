
  
    

    create or replace table `mailshake-analysis-486717`.`GOLD`.`fct_campaign_performance`
      
    
    

    
    OPTIONS()
    as (
      

WITH campaigns AS (
    SELECT 
        campaign_id,
        team_id,
        campaign_title,
        created_at,
        wizard_status,
        sender_email,
        is_archived,
        is_paused
    FROM `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_campaigns`
),

-- Aggregated activity metrics per campaign
activity_metrics AS (
    SELECT
        c.campaign_id,
        
        -- Send metrics
        COUNT(DISTINCT s.recipient_id) AS total_recipients,
        COUNT(s.sent_id) AS emails_sent,
        MIN(s.action_date) AS first_sent_at,
        MAX(s.action_date) AS last_sent_at,
        
        -- Open metrics
        COUNT(o.open_id) AS total_opens,
        COUNT(DISTINCT o.recipient_id) AS unique_openers,
        
        -- Click metrics
        COUNT(cl.click_id) AS total_clicks,
        COUNT(DISTINCT cl.recipient_id) AS unique_clickers,
        
        -- Reply metrics (actual replies only)
        COUNT(CASE WHEN r.reply_type = 'reply' THEN r.reply_id END) AS total_replies,
        COUNT(DISTINCT CASE WHEN r.reply_type = 'reply' THEN r.recipient_id END) AS unique_repliers,
        
        -- Negative signals
        COUNT(CASE WHEN r.reply_type = 'bounce' THEN r.reply_id END) AS total_bounces,
        COUNT(CASE WHEN r.reply_type = 'unsubscribe' THEN r.reply_id END) AS total_unsubscribes
        
    FROM campaigns c
    LEFT JOIN `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_activity_sent` s 
        ON c.campaign_id = s.campaign_id
    LEFT JOIN `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_activity_opens` o 
        ON c.campaign_id = o.campaign_id
    LEFT JOIN `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_activity_clicks` cl 
        ON c.campaign_id = cl.campaign_id
    LEFT JOIN `mailshake-analysis-486717`.`SILVER`.`stg_mailshake_activity_replies` r 
        ON c.campaign_id = r.campaign_id
    GROUP BY c.campaign_id
)

SELECT
    -- Campaign info
    c.campaign_id,
    c.team_id,
    c.campaign_title,
    c.created_at AS campaign_created_at,
    c.wizard_status,
    c.sender_email,
    c.is_archived,
    c.is_paused,
    
    -- Volume metrics
    COALESCE(m.total_recipients, 0) AS total_recipients,
    COALESCE(m.emails_sent, 0) AS emails_sent,
    COALESCE(m.total_opens, 0) AS total_opens,
    COALESCE(m.unique_openers, 0) AS unique_openers,
    COALESCE(m.total_clicks, 0) AS total_clicks,
    COALESCE(m.unique_clickers, 0) AS unique_clickers,
    COALESCE(m.total_replies, 0) AS total_replies,
    COALESCE(m.unique_repliers, 0) AS unique_repliers,
    COALESCE(m.total_bounces, 0) AS total_bounces,
    COALESCE(m.total_unsubscribes, 0) AS total_unsubscribes,
    
    -- Timing
    m.first_sent_at,
    m.last_sent_at,
    
    -- Core rates (0-1 scale for easier charting)
    SAFE_DIVIDE(m.unique_openers, m.emails_sent) AS open_rate,
    SAFE_DIVIDE(m.unique_clickers, m.emails_sent) AS click_rate,
    SAFE_DIVIDE(m.unique_repliers, m.emails_sent) AS reply_rate,
    SAFE_DIVIDE(m.unique_clickers, m.unique_openers) AS click_through_rate,
    SAFE_DIVIDE(m.total_bounces, m.emails_sent) AS bounce_rate,
    SAFE_DIVIDE(m.total_unsubscribes, m.emails_sent) AS unsubscribe_rate,
    
    -- Calculated at timestamp
    CURRENT_TIMESTAMP() AS calculated_at

FROM campaigns c
LEFT JOIN activity_metrics m ON c.campaign_id = m.campaign_id
    );
  