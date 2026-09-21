-- =============================================================================
-- Code Bundles Quickstart — Setup
-- Generates ~50K e-commerce clickstream events + a products dimension table.
-- Events use category-affinity browsing for realistic co-occurrence patterns.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS CB_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS CODE_BUNDLES_DB;
CREATE SCHEMA   IF NOT EXISTS CODE_BUNDLES_DB.PIPELINE;

USE WAREHOUSE CB_WH;
USE DATABASE  CODE_BUNDLES_DB;
USE SCHEMA    PIPELINE;

-- -----------------------------------------------------------------------------
-- Products dimension (200 products across 10 categories)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE PRODUCTS (
  PRODUCT_ID   INTEGER,
  PRODUCT_NAME VARCHAR,
  CATEGORY     VARCHAR,
  PRICE        NUMBER(8, 2)
);

INSERT INTO PRODUCTS
SELECT
  ROW_NUMBER() OVER (ORDER BY SEQ4())                             AS PRODUCT_ID,
  'Product ' || ROW_NUMBER() OVER (ORDER BY SEQ4())               AS PRODUCT_NAME,
  CASE MOD(SEQ4(), 10)
    WHEN 0 THEN 'Electronics'
    WHEN 1 THEN 'Clothing'
    WHEN 2 THEN 'Home & Garden'
    WHEN 3 THEN 'Sports & Outdoors'
    WHEN 4 THEN 'Books'
    WHEN 5 THEN 'Beauty'
    WHEN 6 THEN 'Toys & Games'
    WHEN 7 THEN 'Food & Grocery'
    WHEN 8 THEN 'Automotive'
    WHEN 9 THEN 'Health'
  END                                                             AS CATEGORY,
  ROUND(5.00 + ABS(MOD(HASH(SEQ4(), 'price'), 19500)) / 100.0, 2) AS PRICE
FROM TABLE(GENERATOR(ROWCOUNT => 200));

-- -----------------------------------------------------------------------------
-- Raw clickstream events (~50K rows, 30 days, ~1000 users)
--
-- Design:
-- 1. Each event gets a type by independent probability: 40% page_view,
--    30% product_view, 20% add_to_cart, 10% purchase. No per-session capping,
--    so the aggregate distribution is exactly as stated.
-- 2. Each user has 2-3 preferred categories (out of 10). Products are drawn
--    from these categories, creating sparse co-occurrence — not every product
--    pair will appear together.
-- 3. Events cluster into sessions (bursts of 5-12 events spaced seconds apart).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_EVENTS (
  EVENT_ID       VARCHAR,
  USER_ID        INTEGER,
  EVENT_TIMESTAMP TIMESTAMP_NTZ,
  EVENT_TYPE     VARCHAR,
  PRODUCT_ID     INTEGER,
  CATEGORY       VARCHAR,
  DEVICE         VARCHAR,
  TRAFFIC_SOURCE VARCHAR,
  REVENUE        NUMBER(10, 2)
);

-- Step 1: Session start points with user category affinities
CREATE OR REPLACE TEMPORARY TABLE _SESSION_STARTS AS
SELECT
  SEQ4()                                                   AS session_num,
  MOD(ABS(HASH(SEQ4(), 'user')), 1000) + 1                 AS user_id,
  DATEADD('second',
    -ABS(MOD(HASH(SEQ4(), 'time'), 30 * 86400)),
    CURRENT_TIMESTAMP())                                   AS session_start_ts,
  5 + MOD(ABS(HASH(SEQ4(), 'length')), 8)                  AS session_length,
  CASE
    WHEN MOD(ABS(HASH(SEQ4(), 'device')), 10) < 5 THEN 'desktop'
    WHEN MOD(ABS(HASH(SEQ4(), 'device')), 10) < 8 THEN 'mobile'
    ELSE 'tablet'
  END                                                      AS device,
  CASE MOD(ABS(HASH(SEQ4(), 'source')), 10)
    WHEN 0 THEN 'direct'
    WHEN 1 THEN 'direct'
    WHEN 2 THEN 'email'
    WHEN 3 THEN 'email'
    WHEN 4 THEN 'paid_search'
    WHEN 5 THEN 'paid_search'
    WHEN 6 THEN 'paid_search'
    WHEN 7 THEN 'organic'
    WHEN 8 THEN 'organic'
    WHEN 9 THEN 'social'
  END                                                      AS traffic_source,
  -- User category affinity: each user has ONE primary category.
  -- 85% of their product views come from this category; 15% are random.
  -- This creates sparse co-occurrence — within-category pairs are strong,
  -- cross-category pairs are rare.
  MOD(ABS(HASH(MOD(ABS(HASH(SEQ4(), 'user')), 1000) + 1, 'primary_cat')), 10) AS user_primary_cat
FROM TABLE(GENERATOR(ROWCOUNT => 6000));

-- Step 2: Position sequence (max 12 events per session)
CREATE OR REPLACE TEMPORARY TABLE _POSITIONS AS
SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS event_pos
FROM TABLE(GENERATOR(ROWCOUNT => 12));

-- Step 3: Expand into events — pure probabilistic event type assignment
INSERT INTO RAW_EVENTS
WITH expanded AS (
  SELECT
    s.session_num,
    s.user_id,
    DATEADD('second',
      (p.event_pos - 1) * (10 + MOD(ABS(HASH(s.session_num, p.event_pos, 'gap')), 110)),
      s.session_start_ts
    )                                                       AS event_timestamp,
    -- Pure probability: 40% page_view, 30% product_view, 20% add_to_cart, 10% purchase
    CASE
      WHEN MOD(ABS(HASH(s.session_num, p.event_pos, 'etype')), 100) < 40 THEN 'page_view'
      WHEN MOD(ABS(HASH(s.session_num, p.event_pos, 'etype')), 100) < 70 THEN 'product_view'
      WHEN MOD(ABS(HASH(s.session_num, p.event_pos, 'etype')), 100) < 90 THEN 'add_to_cart'
      ELSE 'purchase'
    END                                                     AS event_type,
    -- Product from user's primary category (95%) or random (5%)
    CASE
      WHEN MOD(ABS(HASH(s.session_num, p.event_pos, 'catpick')), 100) < 95
        THEN s.user_primary_cat * 20 + MOD(ABS(HASH(s.session_num, p.event_pos, 'prod')), 20) + 1
      ELSE MOD(ABS(HASH(s.session_num, p.event_pos, 'randprod')), 200) + 1
    END                                                     AS product_id_raw,
    s.device,
    s.traffic_source
  FROM _SESSION_STARTS s
  JOIN _POSITIONS p ON p.event_pos <= s.session_length
)
SELECT
  UUID_STRING()                                             AS event_id,
  e.user_id,
  e.event_timestamp,
  e.event_type,
  CASE WHEN e.event_type != 'page_view' THEN e.product_id_raw ELSE NULL END AS product_id,
  pr.CATEGORY,
  e.device,
  e.traffic_source,
  CASE
    WHEN e.event_type = 'purchase'
      THEN ROUND(COALESCE(pr.PRICE, 50.00) * (1 + MOD(ABS(HASH(e.session_num, 'qty')), 200) / 100.0), 2)
    ELSE 0.00
  END                                                       AS revenue
FROM expanded e
LEFT JOIN PRODUCTS pr ON
  CASE WHEN e.event_type != 'page_view' THEN e.product_id_raw ELSE NULL END = pr.PRODUCT_ID;

-- Clean up temp tables
DROP TABLE IF EXISTS _SESSION_STARTS;
DROP TABLE IF EXISTS _POSITIONS;

-- Verify
SELECT
  COUNT(*)                              AS total_events,
  COUNT(DISTINCT USER_ID)               AS unique_users,
  COUNT(DISTINCT PRODUCT_ID)            AS unique_products,
  SUM(CASE WHEN EVENT_TYPE = 'page_view' THEN 1 ELSE 0 END)    AS page_views,
  SUM(CASE WHEN EVENT_TYPE = 'product_view' THEN 1 ELSE 0 END) AS product_views,
  SUM(CASE WHEN EVENT_TYPE = 'add_to_cart' THEN 1 ELSE 0 END)  AS add_to_carts,
  SUM(CASE WHEN EVENT_TYPE = 'purchase' THEN 1 ELSE 0 END)     AS purchases
FROM RAW_EVENTS;
