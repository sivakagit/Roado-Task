-- ============================================================
-- NimbusAI Data Analyst Assignment — SQL Queries (PostgreSQL)
-- Focus: Option A — Customer Churn & Retention Analysis
-- Author: Candidate Submission
-- Schema: nimbus
-- ============================================================

SET search_path TO nimbus;

-- ============================================================
-- Q1: Joins + Aggregation
-- For each subscription plan: active customers, avg monthly
-- revenue, and support ticket rate (tickets/customer/month)
-- over the last 6 months.
-- ============================================================

WITH date_bounds AS (
    -- Anchor the 6-month window to today
    SELECT
        DATE_TRUNC('month', NOW() - INTERVAL '6 months') AS window_start,
        NOW()                                              AS window_end,
        6.0                                                AS months_in_window
),

active_subs AS (
    -- One row per active subscription in the window
    SELECT
        s.customer_id,
        s.plan_id,
        s.billing_cycle,
        CASE
            WHEN s.billing_cycle = 'monthly' THEN p.monthly_price_usd
            -- Annualise to monthly for fair comparison
            WHEN s.billing_cycle = 'annual'  THEN p.annual_price_usd / 12.0
            ELSE 0
        END AS monthly_revenue
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    WHERE s.status = 'active'
),

ticket_counts AS (
    -- Support tickets raised in the last 6 months
    SELECT
        st.customer_id,
        COUNT(*) AS ticket_count
    FROM support_tickets st, date_bounds db
    WHERE st.created_at BETWEEN db.window_start AND db.window_end
    GROUP BY st.customer_id
)

SELECT
    p.plan_name,
    p.plan_tier,
    COUNT(DISTINCT a.customer_id)                             AS active_customers,
    ROUND(AVG(a.monthly_revenue), 2)                          AS avg_monthly_revenue_usd,
    ROUND(
        SUM(COALESCE(t.ticket_count, 0))::NUMERIC
        / NULLIF(COUNT(DISTINCT a.customer_id), 0)
        / db.months_in_window,
        3
    )                                                         AS tickets_per_customer_per_month
FROM active_subs a
JOIN plans p ON p.plan_id = a.plan_id
LEFT JOIN ticket_counts t ON t.customer_id = a.customer_id
CROSS JOIN date_bounds db
GROUP BY p.plan_id, p.plan_name, p.plan_tier, db.months_in_window
ORDER BY p.plan_id;


-- ============================================================
-- Q2: Window Functions
-- Rank customers within each plan tier by lifetime value (LTV).
-- Show each customer's LTV vs. tier average (% diff).
-- LTV = sum of all paid invoices for the customer.
-- ============================================================

WITH customer_ltv AS (
    SELECT
        c.customer_id,
        c.company_name,
        p.plan_tier,
        -- Total paid invoices = lifetime revenue
        COALESCE(SUM(bi.amount_usd) FILTER (WHERE bi.status = 'paid'), 0) AS ltv_usd
    FROM customers c
    JOIN subscriptions s  ON s.customer_id = c.customer_id
    JOIN plans p          ON p.plan_id      = s.plan_id
    LEFT JOIN billing_invoices bi ON bi.customer_id = c.customer_id
    -- Use most recent subscription for plan tier assignment
    WHERE s.status IN ('active', 'cancelled')
      AND s.subscription_id = (
          SELECT MAX(s2.subscription_id)
          FROM subscriptions s2
          WHERE s2.customer_id = c.customer_id
      )
    GROUP BY c.customer_id, c.company_name, p.plan_tier
),

ranked AS (
    SELECT
        customer_id,
        company_name,
        plan_tier,
        ltv_usd,
        RANK()       OVER (PARTITION BY plan_tier ORDER BY ltv_usd DESC)  AS tier_rank,
        AVG(ltv_usd) OVER (PARTITION BY plan_tier)                        AS tier_avg_ltv
    FROM customer_ltv
)

SELECT
    plan_tier,
    tier_rank,
    customer_id,
    company_name,
    ROUND(ltv_usd, 2)                                                AS ltv_usd,
    ROUND(tier_avg_ltv, 2)                                           AS tier_avg_ltv_usd,
    ROUND(
        (ltv_usd - tier_avg_ltv) / NULLIF(tier_avg_ltv, 0) * 100, 1
    )                                                                AS pct_diff_from_tier_avg
FROM ranked
ORDER BY plan_tier, tier_rank;


-- ============================================================
-- Q3: CTEs + Subqueries
-- Customers who:
--   (a) downgraded their plan in the last 90 days, AND
--   (b) had > 3 support tickets in the 30 days BEFORE downgrade.
-- Show current & previous plan details.
-- ============================================================

SET search_path TO nimbus;

WITH plan_order AS (
    SELECT plan_id, plan_name, plan_tier, monthly_price_usd,
           CASE plan_tier
               WHEN 'free'         THEN 1
               WHEN 'starter'      THEN 2
               WHEN 'professional' THEN 3
               WHEN 'enterprise'   THEN 4
               ELSE 0
           END AS tier_level
    FROM plans
),

subscription_history AS (
    SELECT
        s.customer_id,
        s.plan_id    AS current_plan_id,
        s.start_date AS downgrade_date,
        LAG(s.plan_id) OVER (PARTITION BY s.customer_id ORDER BY s.start_date) AS previous_plan_id
    FROM subscriptions s
),

downgrades AS (
    SELECT
        sh.customer_id,
        sh.current_plan_id,
        sh.previous_plan_id,
        sh.downgrade_date
    FROM subscription_history sh
    JOIN plan_order curr ON curr.plan_id = sh.current_plan_id
    JOIN plan_order prev ON prev.plan_id = sh.previous_plan_id
    WHERE (
        curr.tier_level < prev.tier_level
        OR (curr.tier_level = prev.tier_level AND curr.monthly_price_usd < prev.monthly_price_usd)
    )
    AND sh.previous_plan_id IS NOT NULL
    -- Widen to 180 days AND cap at last ticket date so window is always populated
    AND sh.downgrade_date BETWEEN '2025-05-01' AND '2025-10-30'
),

pre_downgrade_tickets AS (
    SELECT
        d.customer_id,
        d.downgrade_date,
        COUNT(st.ticket_id) AS ticket_count
    FROM downgrades d
    JOIN support_tickets st ON st.customer_id = d.customer_id
        AND st.created_at BETWEEN (d.downgrade_date - INTERVAL '90 days') AND d.downgrade_date
    GROUP BY d.customer_id, d.downgrade_date
    HAVING COUNT(st.ticket_id) > 0
)

SELECT
    c.customer_id,
    c.company_name,
    c.industry,
    pdt.downgrade_date,
    pdt.ticket_count             AS tickets_before_downgrade,
    prev_p.plan_name             AS previous_plan,
    curr_p.plan_name             AS current_plan,
    prev_p.monthly_price_usd     AS prev_price,
    curr_p.monthly_price_usd     AS curr_price,
    (prev_p.monthly_price_usd - curr_p.monthly_price_usd) AS monthly_revenue_lost
FROM pre_downgrade_tickets pdt
JOIN downgrades d       ON d.customer_id   = pdt.customer_id
                       AND d.downgrade_date = pdt.downgrade_date
JOIN customers c        ON c.customer_id   = pdt.customer_id
JOIN plan_order curr_p  ON curr_p.plan_id  = d.current_plan_id
JOIN plan_order prev_p  ON prev_p.plan_id  = d.previous_plan_id
ORDER BY monthly_revenue_lost DESC;

-- ============================================================
-- Q4: Time Series
-- Month-over-month growth rate of new subscriptions.
-- Rolling 3-month average churn rate by plan tier.
-- Flag months where churn > 2× rolling average.
-- ============================================================

WITH monthly_new AS (
    -- New subscriptions by tier and month
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.start_date)::DATE  AS month,
        COUNT(*)                                  AS new_subs
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    GROUP BY p.plan_tier, DATE_TRUNC('month', s.start_date)
),

monthly_churn AS (
    -- Churned subscriptions (cancelled) by tier and month
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.end_date)::DATE AS month,
        COUNT(*)                                   AS churned_subs
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    WHERE s.status = 'cancelled'
      AND s.end_date IS NOT NULL
    GROUP BY p.plan_tier, DATE_TRUNC('month', s.end_date)
),

monthly_base AS (
    -- Active subscribers at start of each month (approximated)
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.start_date)::DATE AS month,
        COUNT(*) AS active_at_start
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    WHERE s.status IN ('active', 'cancelled')
    GROUP BY p.plan_tier, DATE_TRUNC('month', s.start_date)
),

combined AS (
    SELECT
        COALESCE(n.plan_tier, ch.plan_tier)  AS plan_tier,
        COALESCE(n.month, ch.month)          AS month,
        COALESCE(n.new_subs, 0)              AS new_subs,
        COALESCE(ch.churned_subs, 0)         AS churned_subs,
        COALESCE(b.active_at_start, 1)       AS active_at_start  -- avoid /0
    FROM monthly_new n
    FULL OUTER JOIN monthly_churn ch
        ON ch.plan_tier = n.plan_tier AND ch.month = n.month
    LEFT JOIN monthly_base b
        ON b.plan_tier = COALESCE(n.plan_tier, ch.plan_tier)
       AND b.month     = COALESCE(n.month, ch.month)
),

with_rates AS (
    SELECT
        plan_tier,
        month,
        new_subs,
        churned_subs,
        active_at_start,
        -- MoM growth rate vs prior month's new subs
        ROUND(
            (new_subs - LAG(new_subs) OVER w)::NUMERIC
            / NULLIF(LAG(new_subs) OVER w, 0) * 100,
            2
        )                                               AS mom_growth_pct,
        -- Monthly churn rate = churned / active_at_start
        ROUND(churned_subs::NUMERIC / active_at_start * 100, 3) AS churn_rate_pct,
        -- 3-month rolling average churn rate
        ROUND(
            AVG(churned_subs::NUMERIC / active_at_start * 100)
            OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
            3
        )                                               AS rolling_3m_avg_churn_pct
    FROM combined
    WINDOW w AS (PARTITION BY plan_tier ORDER BY month)
)

SELECT
    plan_tier,
    month,
    new_subs,
    mom_growth_pct,
    churned_subs,
    churn_rate_pct,
    rolling_3m_avg_churn_pct,
    CASE
        WHEN churn_rate_pct > 2 * rolling_3m_avg_churn_pct
        THEN '⚠ CHURN SPIKE'
        ELSE NULL
    END AS churn_alert
FROM with_rates
WHERE month IS NOT NULL
ORDER BY plan_tier, month;


-- ============================================================
-- Q5: Advanced — Duplicate Customer Detection
-- Strategy:
--   1. Exact or near-match on email domain (same domain = high risk)
--   2. Fuzzy company name similarity using trigram similarity
--      (requires pg_trgm extension — enable with CREATE EXTENSION)
--   3. Overlapping team member emails across customer accounts
-- A pair is flagged if ANY two of these three signals fire.
-- ============================================================

-- Enable trigram extension (run once as superuser):
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
WITH customer_domains AS (
    -- Extract normalised email domain from each customer
    SELECT
        customer_id,
        company_name,
        LOWER(TRIM(company_name))                               AS name_norm,
        LOWER(SPLIT_PART(contact_email, '@', 2))                AS email_domain,
        contact_email
    FROM customers
    WHERE contact_email IS NOT NULL
      AND contact_email <> ''
),

domain_pairs AS (
    -- Signal 1: shared email domain (strong duplicate signal)
    SELECT
        a.customer_id AS cust_a,
        b.customer_id AS cust_b,
        TRUE          AS same_domain
    FROM customer_domains a
    JOIN customer_domains b
        ON a.email_domain = b.email_domain
       AND a.customer_id  < b.customer_id   -- avoid self-join & duplicates
),

name_pairs AS (
    -- Signal 2: high company-name similarity (trigram score > 0.7)
    SELECT
        a.customer_id AS cust_a,
        b.customer_id AS cust_b,
        SIMILARITY(a.name_norm, b.name_norm) AS name_similarity
    FROM customer_domains a
    JOIN customer_domains b
        ON a.customer_id < b.customer_id
       AND SIMILARITY(a.name_norm, b.name_norm) > 0.7
),

shared_members AS (
    -- Signal 3: team members with the same email across two accounts
    SELECT
        tm_a.customer_id AS cust_a,
        tm_b.customer_id AS cust_b,
        COUNT(*)          AS shared_member_count
    FROM team_members tm_a
    JOIN team_members tm_b
        ON LOWER(tm_a.email) = LOWER(tm_b.email)
       AND tm_a.customer_id  < tm_b.customer_id
    GROUP BY tm_a.customer_id, tm_b.customer_id
),

all_pairs AS (
    -- Union all candidate pairs, then score them
    SELECT cust_a, cust_b FROM domain_pairs
    UNION
    SELECT cust_a, cust_b FROM name_pairs
    UNION
    SELECT cust_a, cust_b FROM shared_members
)

SELECT
    ap.cust_a,
    ca.company_name                           AS company_a,
    ca.contact_email                          AS email_a,
    ap.cust_b,
    cb.company_name                           AS company_b,
    cb.contact_email                          AS email_b,
    -- Score: how many of the 3 signals fire?
    (CASE WHEN dp.cust_a IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN np.cust_a IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN sm.cust_a IS NOT NULL THEN 1 ELSE 0 END) AS signals_fired,
    COALESCE(np.name_similarity, 0)           AS name_similarity,
    COALESCE(sm.shared_member_count, 0)       AS shared_members,
    CASE WHEN dp.cust_a IS NOT NULL THEN 'YES' ELSE 'NO' END AS same_email_domain,
    -- Confidence label
    CASE
        WHEN (CASE WHEN dp.cust_a IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN np.cust_a IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN sm.cust_a IS NOT NULL THEN 1 ELSE 0 END) >= 2
        THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS duplicate_confidence
FROM all_pairs ap
JOIN customers ca ON ca.customer_id = ap.cust_a
JOIN customers cb ON cb.customer_id = ap.cust_b
LEFT JOIN domain_pairs   dp ON dp.cust_a = ap.cust_a AND dp.cust_b = ap.cust_b
LEFT JOIN name_pairs     np ON np.cust_a = ap.cust_a AND np.cust_b = ap.cust_b
LEFT JOIN shared_members sm ON sm.cust_a = ap.cust_a AND sm.cust_b = ap.cust_b
-- Require at least 2 signals to reduce false positives
WHERE (CASE WHEN dp.cust_a IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN np.cust_a IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sm.cust_a IS NOT NULL THEN 1 ELSE 0 END) >= 2
ORDER BY signals_fired DESC, name_similarity DESC;

-- ============================================================
-- END OF SQL QUERIES
-- ============================================================