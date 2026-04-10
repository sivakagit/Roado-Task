-- ============================================================
-- NimbusAI — Task 1: SQL Queries (PostgreSQL)
-- Focus Area: Option A — Customer Churn & Retention Analysis
-- Schema: nimbus.*
-- ============================================================

SET search_path TO nimbus;

-- ============================================================
-- Q1: Joins + Aggregation
-- For each subscription plan, calculate:
--   - Number of active customers
--   - Average monthly revenue (MRR)
--   - Support ticket rate (tickets per customer per month)
-- Over the last 6 months
-- ============================================================

WITH active_subs AS (
    -- Get customers with an active subscription in the last 6 months
    SELECT
        s.customer_id,
        s.plan_id,
        s.mrr_usd
    FROM subscriptions s
    WHERE s.status = 'active'
      AND s.start_date >= CURRENT_DATE - INTERVAL '6 months'
),
ticket_counts AS (
    -- Count support tickets per customer in the last 6 months
    SELECT
        customer_id,
        COUNT(*) AS ticket_count
    FROM support_tickets
    WHERE created_at >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY customer_id
),
monthly_span AS (
    -- Number of months in the window (used for rate calculation)
    SELECT 6.0 AS months
)
SELECT
    p.plan_name,
    p.plan_tier,
    COUNT(DISTINCT a.customer_id)                          AS active_customers,
    ROUND(AVG(a.mrr_usd), 2)                               AS avg_mrr_usd,
    ROUND(
        SUM(COALESCE(t.ticket_count, 0))::NUMERIC
        / NULLIF(COUNT(DISTINCT a.customer_id), 0)
        / (SELECT months FROM monthly_span),
        4
    )                                                       AS tickets_per_customer_per_month
FROM active_subs a
JOIN plans p ON a.plan_id = p.plan_id
LEFT JOIN ticket_counts t ON a.customer_id = t.customer_id
GROUP BY p.plan_id, p.plan_name, p.plan_tier
ORDER BY p.monthly_price_usd DESC NULLS LAST;


-- ============================================================
-- Q2: Window Functions
-- Rank customers within each plan tier by total lifetime value (LTV).
-- LTV = sum of all MRR paid across subscriptions.
-- Also show % difference from their tier's average LTV.
-- ============================================================

WITH customer_ltv AS (
    -- Calculate LTV per customer: sum of mrr across all subscription months
    -- Approximation: mrr * months active per subscription
    SELECT
        s.customer_id,
        p.plan_tier,
        SUM(
            s.mrr_usd *
            EXTRACT(MONTH FROM AGE(
                COALESCE(s.end_date, CURRENT_DATE),
                s.start_date
            ))
        ) AS lifetime_value_usd
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    WHERE s.mrr_usd > 0
    GROUP BY s.customer_id, p.plan_tier
),
ranked AS (
    SELECT
        c.company_name,
        l.plan_tier,
        ROUND(l.lifetime_value_usd::NUMERIC, 2)            AS ltv_usd,
        RANK() OVER (
            PARTITION BY l.plan_tier
            ORDER BY l.lifetime_value_usd DESC
        )                                                   AS rank_in_tier,
        ROUND(AVG(l.lifetime_value_usd) OVER (
            PARTITION BY l.plan_tier
        )::NUMERIC, 2)                                      AS tier_avg_ltv_usd
    FROM customer_ltv l
    JOIN customers c ON l.customer_id = c.customer_id
)
SELECT
    company_name,
    plan_tier,
    ltv_usd,
    rank_in_tier,
    tier_avg_ltv_usd,
    ROUND(
        (ltv_usd - tier_avg_ltv_usd) / NULLIF(tier_avg_ltv_usd, 0) * 100,
        2
    )                                                       AS pct_diff_from_tier_avg
FROM ranked
ORDER BY plan_tier, rank_in_tier;


-- ============================================================
-- Q3: CTEs + Subqueries
-- Customers who:
--   1. Downgraded their plan in the last 90 days
--   2. Had more than 3 support tickets in the 30 days BEFORE downgrading
-- Include current and previous plan details.
-- ============================================================

WITH plan_price AS (
    -- Helper: map plan_id to monthly price for upgrade/downgrade detection
    SELECT plan_id, plan_name, plan_tier, monthly_price_usd
    FROM plans
),
downgrades AS (
    -- Find customers who switched to a cheaper plan in the last 90 days
    -- by joining consecutive subscriptions for the same customer
    SELECT
        s_new.customer_id,
        s_new.plan_id                          AS new_plan_id,
        s_old.plan_id                          AS old_plan_id,
        s_new.start_date                       AS downgrade_date
    FROM subscriptions s_new
    JOIN subscriptions s_old
        ON s_new.customer_id = s_old.customer_id
       AND s_old.end_date::DATE = s_new.start_date::DATE   -- old ended when new started
    JOIN plan_price pp_new ON s_new.plan_id = pp_new.plan_id
    JOIN plan_price pp_old ON s_old.plan_id = pp_old.plan_id
    WHERE pp_new.monthly_price_usd < pp_old.monthly_price_usd  -- price went down = downgrade
      AND s_new.start_date >= CURRENT_DATE - INTERVAL '90 days'
),
tickets_before_downgrade AS (
    -- Count tickets for each downgraded customer in 30 days before downgrade
    SELECT
        d.customer_id,
        d.downgrade_date,
        d.new_plan_id,
        d.old_plan_id,
        COUNT(st.ticket_id) AS tickets_30d_before
    FROM downgrades d
    JOIN support_tickets st
        ON st.customer_id = d.customer_id
       AND st.created_at >= (d.downgrade_date - INTERVAL '30 days')
       AND st.created_at <  d.downgrade_date
    GROUP BY d.customer_id, d.downgrade_date, d.new_plan_id, d.old_plan_id
    HAVING COUNT(st.ticket_id) > 3
)
SELECT
    c.company_name,
    c.contact_email,
    c.industry,
    c.company_size,
    t.downgrade_date,
    t.tickets_30d_before,
    pp_old.plan_name                           AS previous_plan,
    pp_old.plan_tier                           AS previous_tier,
    pp_old.monthly_price_usd                   AS previous_price_usd,
    pp_new.plan_name                           AS current_plan,
    pp_new.plan_tier                           AS current_tier,
    pp_new.monthly_price_usd                   AS current_price_usd
FROM tickets_before_downgrade t
JOIN customers c      ON t.customer_id  = c.customer_id
JOIN plan_price pp_old ON t.old_plan_id = pp_old.plan_id
JOIN plan_price pp_new ON t.new_plan_id = pp_new.plan_id
ORDER BY t.tickets_30d_before DESC, t.downgrade_date DESC;


-- ============================================================
-- Q4: Time Series
-- Month-over-month growth rate of new subscriptions,
-- rolling 3-month average churn rate, both by plan tier.
-- Flag months where churn exceeded 2x the rolling average.
-- ============================================================

WITH monthly_new AS (
    -- New subscriptions per month and tier
    SELECT
        DATE_TRUNC('month', s.start_date)      AS month,
        p.plan_tier,
        COUNT(*)                               AS new_subs
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    WHERE s.start_date IS NOT NULL
    GROUP BY 1, 2
),
monthly_churn AS (
    -- Churned subscriptions per month and tier
    -- A churn event = subscription ended (cancelled or expired) in that month
    SELECT
        DATE_TRUNC('month', s.end_date::DATE)  AS month,
        p.plan_tier,
        COUNT(*)                               AS churned_subs
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    WHERE s.end_date IS NOT NULL
      AND s.status IN ('cancelled', 'expired')
    GROUP BY 1, 2
),
combined AS (
    SELECT
        COALESCE(n.month, c.month)             AS month,
        COALESCE(n.plan_tier, c.plan_tier)     AS plan_tier,
        COALESCE(n.new_subs, 0)                AS new_subs,
        COALESCE(c.churned_subs, 0)            AS churned_subs
    FROM monthly_new n
    FULL OUTER JOIN monthly_churn c
        ON n.month = c.month AND n.plan_tier = c.plan_tier
),
with_rates AS (
    SELECT
        month,
        plan_tier,
        new_subs,
        churned_subs,
        -- MoM growth rate of new subscriptions
        ROUND(
            (new_subs - LAG(new_subs) OVER (PARTITION BY plan_tier ORDER BY month))::NUMERIC
            / NULLIF(LAG(new_subs) OVER (PARTITION BY plan_tier ORDER BY month), 0) * 100,
            2
        )                                      AS mom_new_subs_growth_pct,
        -- Rolling 3-month average churn
        ROUND(AVG(churned_subs) OVER (
            PARTITION BY plan_tier
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2)                         AS rolling_3m_avg_churn
    FROM combined
)
SELECT
    month,
    plan_tier,
    new_subs,
    churned_subs,
    mom_new_subs_growth_pct,
    rolling_3m_avg_churn,
    -- Flag months where actual churn > 2x rolling average
    CASE
        WHEN churned_subs > 2 * rolling_3m_avg_churn THEN 'CHURN SPIKE'
        ELSE 'normal'
    END                                        AS churn_flag
FROM with_rates
WHERE month IS NOT NULL
ORDER BY plan_tier, month;


-- ============================================================
-- Q5: Advanced — Duplicate Customer Detection
-- Detect potential duplicate accounts based on:
--   1. Similar company names (trigram similarity via pg_trgm)
--   2. Same email domain
--   3. Overlapping team members (shared email addresses)
--
-- Matching logic:
--   - We use pg_trgm similarity for fuzzy name matching (threshold 0.5)
--   - Domain extracted from contact_email
--   - Team member overlap checked via inner join on email addresses
--   - Two or more signals = likely duplicate
-- ============================================================

-- NOTE: Enable pg_trgm extension if not already done:
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;

WITH customer_domains AS (
    -- Extract email domain per customer
    SELECT
        customer_id,
        company_name,
        contact_email,
        LOWER(SPLIT_PART(contact_email, '@', 2)) AS email_domain,
        signup_date,
        is_active
    FROM customers
),
name_similar_pairs AS (
    -- Pairs of customers whose company names are trigram-similar (score >= 0.5)
    -- and are NOT the same customer
    SELECT
        a.customer_id   AS cust_a,
        b.customer_id   AS cust_b,
        a.company_name  AS name_a,
        b.company_name  AS name_b,
        ROUND(similarity(a.company_name, b.company_name)::NUMERIC, 3) AS name_similarity,
        CASE WHEN a.email_domain = b.email_domain THEN TRUE ELSE FALSE END AS same_domain
    FROM customer_domains a
    JOIN customer_domains b
        ON a.customer_id < b.customer_id   -- avoid self-join and duplicate pairs
       AND similarity(a.company_name, b.company_name) >= 0.5
),
member_overlap AS (
    -- Pairs of customers who share at least one team member email
    SELECT
        tm1.customer_id AS cust_a,
        tm2.customer_id AS cust_b,
        COUNT(*)        AS shared_member_count
    FROM team_members tm1
    JOIN team_members tm2
        ON LOWER(tm1.email) = LOWER(tm2.email)
       AND tm1.customer_id < tm2.customer_id
    GROUP BY tm1.customer_id, tm2.customer_id
    HAVING COUNT(*) >= 1
)
SELECT
    n.cust_a,
    c_a.company_name                           AS company_a,
    c_a.contact_email                          AS email_a,
    c_a.signup_date                            AS signup_a,
    n.cust_b,
    c_b.company_name                           AS company_b,
    c_b.contact_email                          AS email_b,
    c_b.signup_date                            AS signup_b,
    n.name_similarity,
    n.same_domain,
    COALESCE(m.shared_member_count, 0)         AS shared_members,
    -- Score: how many signals match (max 3)
    (CASE WHEN n.name_similarity >= 0.7 THEN 1 ELSE 0 END
     + CASE WHEN n.same_domain THEN 1 ELSE 0 END
     + CASE WHEN m.shared_member_count > 0 THEN 1 ELSE 0 END
    )                                          AS duplicate_signal_score
FROM name_similar_pairs n
JOIN customers c_a ON n.cust_a = c_a.customer_id
JOIN customers c_b ON n.cust_b = c_b.customer_id
LEFT JOIN member_overlap m
    ON n.cust_a = m.cust_a AND n.cust_b = m.cust_b
ORDER BY duplicate_signal_score DESC, n.name_similarity DESC;
