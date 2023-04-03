# GLOBAL VARIABLES
DECLARE date_from, date_to DATE;
DECLARE sites, content_sources, priorities, campaign_targets ARRAY<STRING>;
SET date_from = CURRENT_DATE -14; 
SET date_to = CURRENT_DATE -1;
SET sites = ['MCO','MLA','MLB','MLC','MLM','MLU','MPE'];
SET content_sources = ['displayCoreApi','DSP','n/a'];
SET priorities = ['ADSALES1','ADSALES2','HOUSE','SPONSOR','SUPERHOUSE','bidding','n/a'];
SET campaign_targets = ['CPA','CPC','IMPRESSIONS','REACH','n/a'];

# BASE TABLE
WITH base_table AS (
  WITH days AS (
    SELECT date_day
    FROM UNNEST(GENERATE_DATE_ARRAY(date_from, date_to, INTERVAL 1 DAY)) date_day
  ),
  sites AS (
    SELECT site
    FROM UNNEST(sites) site
  ),
  content_sources AS (
    SELECT content_source
    FROM UNNEST(content_sources) content_source
  ),
  priorities AS (
    SELECT priority
    FROM UNNEST(priorities) priority
  ),
  advertiser_categories AS (
      SELECT DISTINCT category advertiser_category
      FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.advertiser`

      UNION DISTINCT
      SELECT 'n/a' advertiser_category
  ),
  placements AS (
      SELECT DISTINCT JSON_EXTRACT_SCALAR(event_data, '$.placement') placement
      FROM `meli-bi-data.MELIDATA.ADVERTISING`
      WHERE ds BETWEEN date_from AND date_to
        AND event IN ('display_prints')

      UNION DISTINCT
      SELECT 'n/a' placement
  ),
  campaign_targets AS (
      SELECT campaign_target
      FROM UNNEST(campaign_targets) campaign_target
  )
  SELECT 
      date_day, 
      site,
      content_source,
      priority,
      advertiser_category, 
      placement, 
      campaign_target 
  FROM days, sites, content_sources, priorities, advertiser_categories, placements, campaign_targets
),

# AUX TABLE FOR ADVERTISER CATEGORY
advertiser_categories AS (
  SELECT DISTINCT
    SAFE_CAST(advertiser_id AS STRING) advertiser_id,
    category advertiser_category
  FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.advertiser`
),

# AUX TABLE FOR CAMPAIGN TARGET
campaign_targets AS (
  SELECT DISTINCT
    site_id site,
    SAFE_CAST(campaign_id AS STRING) campaign_id,
    UPPER(JSON_EXTRACT_SCALAR(PARSE_JSON(STRING(goal)), '$.type')) campaign_target,
    UPPER(JSON_EXTRACT_SCALAR(PARSE_JSON(STRING(goal)), '$.strategy')) campaign_strategy
  FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.campaign`
),

# DISPLAY PRINTS
display_prints AS (
  SELECT
    DATE(ads.ds) date_day,
    ads.site,
    COALESCE(JSON_EXTRACT_SCALAR(event_data, '$.content_source'), 'n/a') content_source,
    COALESCE(JSON_EXTRACT_SCALAR(event_data, '$.priority'), 'n/a') priority,
    COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.placement'), 'n/a') placement,
    COALESCE(ct.campaign_target, 'n/a') campaign_target,
    COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.print_id')) total_impressions,
    SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.impression_cost') AS FLOAT64)) impression_cost_lc,
    SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.impression_cost_usd') AS FLOAT64)) impression_cost_usd
  FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

  LEFT JOIN advertiser_categories adv
         ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

  LEFT JOIN campaign_targets ct
         ON ct.site = ads.site
        AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')

  WHERE ads.ds BETWEEN date_from AND date_to
    AND ads.site IN UNNEST(sites)
    AND ads.event = 'display_prints'
    AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

  GROUP BY 1,2,3,4,5,6,7
),

# DISPLAY PRINTS EMPTY
display_empty_prints AS (
  SELECT
    DATE(ds) date_day,
    site,
    'n/a' content_source,
    'n/a' priority,
    'n/a' advertiser_category,
    'n/a' placement, # JSON_EXTRACT_SCALAR(event_data, '$.placement') for now is incompatible with display_prints placements
    'n/a' campaign_target,
    COUNT(1) total_empty_impressions
  FROM `meli-bi-data.MELIDATA.ADVERTISING`

  WHERE ds BETWEEN date_from AND date_to
    AND site IN UNNEST(sites)
    AND event = 'display_empty_prints'
    AND JSON_EXTRACT_SCALAR(event_data, '$.reason') IN ('NO_INVENTORY', 'NO_MATCHING_CONTENT', 'AD_COLLISION')
    AND JSON_EXTRACT_SCALAR(event_data, '$.valid') = 'true'

  GROUP BY 1,2,3,4,5,6,7
),

# DISPLAY CLICKS
display_clicks AS (
  WITH print_priority AS (
    SELECT DISTINCT
      DATE(ads.ds) date_day,
      ads.site,
      JSON_EXTRACT_SCALAR(ads.event_data, '$.print_id') print_id,
      COALESCE(JSON_EXTRACT_SCALAR(event_data, '$.priority'), 'n/a') priority    
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'
  )
  SELECT
    DATE(ads.ds) date_day,
    ads.site,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.content_source'), 'n/a') content_source,
    COALESCE(pp.priority, 'n/a') priority,
    COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.placement'), 'n/a') placement,
    COALESCE(ct.campaign_target, 'n/a') campaign_target,
    COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.click_id')) clicks
  FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

  LEFT JOIN advertiser_categories adv
         ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

  LEFT JOIN campaign_targets ct
         ON ct.site = ads.site
        AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')

  LEFT JOIN print_priority pp
         ON pp.date_day = DATE(ads.ds)
        AND pp.site = ads.site
        AND pp.print_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.print_id')

  WHERE ads.ds BETWEEN date_from AND date_to
    AND ads.site IN UNNEST(sites)
    AND ads.event = 'display_clicks'
    
  GROUP BY 1,2,3,4,5,6,7
),

# DISPLAY ORDERS BY ATTRIBUTION DATE
display_attribution_orders_att AS (
  # orders grouped by the attributed click/print date (user_local_timestamp)
  SELECT
    DATE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].user_local_timestamp')) date_day,
    ads.site,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.content_source'), 'n/a') content_source,
    'n/a' priority,
    COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.placement'), 'n/a') placement,
    COALESCE(ct.campaign_target, 'n/a') campaign_target,
    COUNT(DISTINCT SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.order_id') AS INT64)) orders_att,
    COUNT(DISTINCT IF(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].path') IN ('/display/prints', '/display/views'), SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.order_id') AS INT64), NULL)) orders_print_att,
    COUNT(DISTINCT IF(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].path') = '/display/clicks', SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.order_id') AS INT64), NULL)) orders_click_att,
    SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.total_amount') AS FLOAT64)) orders_total_amount_lc_att,
    SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.total_amount') AS FLOAT64) / cur.CCO_TC_VALUE) orders_total_amount_usd_att
  FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

  LEFT JOIN advertiser_categories adv
         ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.advertiser_id')

  LEFT JOIN campaign_targets ct
         ON ct.site = ads.site
        AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.campaign_id')

  LEFT JOIN `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` cur
         ON cur.TIM_DAY_TC = DATE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].user_local_timestamp'))
        AND TRIM(cur.SIT_SITE_ID) = ads.site
        AND cur.CCO_TO_CURRENCY_ID IN ('COP','ARS','BRL','CLP','MXN','UYU','PEN')

  WHERE ads.ds BETWEEN date_from AND date_to
    AND DATE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].user_local_timestamp')) BETWEEN date_from AND date_to
    AND cur.TIM_DAY_TC BETWEEN date_from AND date_to
    AND ads.site IN UNNEST(sites)
    AND ads.event = 'display_attribution_orders'

  GROUP BY 1,2,3,4,5,6,7
),

# DISPLAY ORDERS BY CONVERTION DATE
display_attribution_orders_cre AS (
  # orders grouped by the conversion date (conversion.create_time)
  SELECT
    DATE(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.create_time')) date_day,
    ads.site,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.content_source'), 'n/a') content_source,
    'n/a' priority,
    COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
    COALESCE(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.placement'), 'n/a') placement,
    COALESCE(ct.campaign_target, 'n/a') campaign_target,
    COUNT(DISTINCT SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.order_id') AS INT64)) orders_cre,
    COUNT(DISTINCT IF(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].path') IN ('/display/prints', '/display/views'), SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.order_id') AS INT64), NULL)) orders_print_cre,
    COUNT(DISTINCT IF(JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].path') = '/display/clicks', SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.order_id') AS INT64), NULL)) orders_click_cre,
    SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.total_amount') AS FLOAT64)) orders_total_amount_lc_cre,
    SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.total_amount') AS FLOAT64) / cur.CCO_TC_VALUE) orders_total_amount_usd_cre
  FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

  LEFT JOIN advertiser_categories adv
         ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.advertiser_id')

  LEFT JOIN campaign_targets ct
         ON ct.site = ads.site
        AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.events[0].event_data.campaign_id')

  LEFT JOIN `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` cur
         ON cur.TIM_DAY_TC = DATE(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.create_time'))
        AND TRIM(cur.SIT_SITE_ID) = ads.site
        AND cur.CCO_TO_CURRENCY_ID IN ('COP','ARS','BRL','CLP','MXN','UYU','PEN')

  WHERE ads.ds BETWEEN date_from AND date_to
    AND DATE(JSON_EXTRACT_SCALAR(ads.event_data, '$.conversion.create_time')) BETWEEN date_from AND date_to
    AND cur.TIM_DAY_TC BETWEEN date_from AND date_to
    AND ads.site IN UNNEST(sites)
    AND ads.event = 'display_attribution_orders'

  GROUP BY 1,2,3,4,5,6,7
),

# DISPLAY ADVERTISERS
display_advertisers AS (
  WITH advertisers_base AS (
    WITH days AS (
      SELECT date_day
      FROM UNNEST(GENERATE_DATE_ARRAY(date_from, date_to, INTERVAL 1 DAY)) date_day
    ),
    sites AS (
      SELECT site
      FROM UNNEST(sites) site
    ),
    advertiser_categories AS (
      SELECT DISTINCT category advertiser_category
      FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.advertiser`

      UNION DISTINCT
      SELECT 'n/a' advertiser_category
    )
    SELECT
      date_day,
      site,
      advertiser_category
    FROM days, sites, advertiser_categories
  ),
  advertisers_day AS (
    SELECT
      DATE(ads.ds) date_day,
      ads.site,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')) total_advertisers_day
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
           ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

    GROUP BY 1,2,3
  ),
  advertisers_week AS (
    SELECT
      DATE_TRUNC(DATE(ads.ds), ISOWEEK) date_week,
      ads.site,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')) total_advertisers_week
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
           ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

    GROUP BY 1,2,3
  ),
  advertisers_month AS (
    SELECT
      DATE_TRUNC(DATE(ads.ds), MONTH) date_month,
      ads.site,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')) total_advertisers_month
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
           ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

    GROUP BY 1,2,3
  )
  SELECT
    a_base.date_day,
    a_base.site,
    'n/a' content_source,
    'n/a' priority,
    a_base.advertiser_category,
    'n/a' placement,
    'n/a' campaign_target,
    COALESCE(ac_day.total_advertisers_day, 0) total_advertisers_day,
    COALESCE(ac_week.total_advertisers_week, 0) total_advertisers_week,
    COALESCE(ac_month.total_advertisers_month, 0) total_advertisers_month
  FROM advertisers_base a_base

  LEFT JOIN advertisers_day ac_day
         ON ac_day.date_day = a_base.date_day
        AND ac_day.site = a_base.site
        AND ac_day.advertiser_category = a_base.advertiser_category

  LEFT JOIN advertisers_week ac_week
         ON ac_week.date_week = a_base.date_day
        AND ac_week.site = a_base.site
        AND ac_week.advertiser_category = a_base.advertiser_category

  LEFT JOIN advertisers_month ac_month
         ON ac_month.date_month = a_base.date_day
        AND ac_month.site = a_base.site
        AND ac_month.advertiser_category = a_base.advertiser_category

  WHERE COALESCE(
    ac_day.total_advertisers_day,
    ac_week.total_advertisers_week,
    ac_month.total_advertisers_month
  ) IS NOT NULL
),

# DISPLAY CAMPAIGNS
display_campaigns AS (
  WITH campaigns_base AS (
    WITH days AS (
      SELECT date_day
      FROM UNNEST(GENERATE_DATE_ARRAY(date_from, date_to, INTERVAL 1 DAY)) date_day
    ),
    sites AS (
        SELECT site
        FROM UNNEST(sites) site
    ),
    content_sources AS (
      SELECT content_source
      FROM UNNEST(ARRAY['displayCoreApi','DSP','n/a']) content_source
    ),
    advertiser_categories AS (
        SELECT DISTINCT category advertiser_category
        FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.advertiser`

        UNION DISTINCT
        SELECT 'n/a' advertiser_category
    ),
    campaign_targets AS (
        SELECT campaign_target
        FROM UNNEST(campaign_targets) campaign_target
    )
    SELECT
      date_day,
      site,
      content_source,
      advertiser_category,
      campaign_target
    FROM days, sites, content_sources, advertiser_categories, campaign_targets
  ),  
  campaigns_day AS (
    SELECT
      DATE(ads.ds) date_day,
      ads.site,
      COALESCE(JSON_EXTRACT_SCALAR(event_data, '$.content_source'), 'n/a') content_source,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      COALESCE(ct.campaign_target, 'n/a') campaign_target,
      COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')) total_campaigns_day
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
           ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    LEFT JOIN campaign_targets ct
           ON ct.site = ads.site
          AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

    GROUP BY 1,2,3,4,5
  ),
  campaigns_week AS (
    SELECT
      DATE_TRUNC(DATE(ads.ds), ISOWEEK) date_week,
      ads.site,
      COALESCE(JSON_EXTRACT_SCALAR(event_data, '$.content_source'), 'n/a') content_source,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      COALESCE(ct.campaign_target, 'n/a') campaign_target,
      COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')) total_campaigns_week
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
           ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    LEFT JOIN campaign_targets ct
           ON ct.site = ads.site
          AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

    GROUP BY 1,2,3,4,5
  ),
  campaigns_month AS (
    SELECT
      DATE_TRUNC(DATE(ads.ds), MONTH) date_month,
      ads.site,
      COALESCE(JSON_EXTRACT_SCALAR(event_data, '$.content_source'), 'n/a') content_source,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      COALESCE(ct.campaign_target, 'n/a') campaign_target,
      COUNT(DISTINCT JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')) total_campaigns_month
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
           ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    LEFT JOIN campaign_targets ct
           ON ct.site = ads.site
          AND ct.campaign_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.campaign_id')

    WHERE ads.ds BETWEEN date_from AND date_to
      AND ads.site IN UNNEST(sites)
      AND ads.event = 'display_prints'
      AND JSON_EXTRACT_SCALAR(ads.event_data, '$.valid') = 'true'

    GROUP BY 1,2,3,4,5
  )
  SELECT
    c_base.date_day,
    c_base.site,
    c_base.content_source,
    'n/a' priority,
    c_base.advertiser_category,
    'n/a' placement,
    c_base.campaign_target,
    COALESCE(ca_day.total_campaigns_day, 0) total_campaigns_day,
    COALESCE(ca_week.total_campaigns_week, 0) total_campaigns_week,
    COALESCE(ca_month.total_campaigns_month, 0) total_campaigns_month
  FROM campaigns_base c_base

  LEFT JOIN campaigns_day ca_day
         ON ca_day.date_day = c_base.date_day
        AND ca_day.site = c_base.site
        AND ca_day.content_source = c_base.content_source
        AND ca_day.advertiser_category = c_base.advertiser_category
        AND ca_day.campaign_target = c_base.campaign_target

  LEFT JOIN campaigns_week ca_week
         ON ca_week.date_week = c_base.date_day
        AND ca_week.site = c_base.site
        AND ca_week.content_source = c_base.content_source
        AND ca_week.advertiser_category = c_base.advertiser_category
        AND ca_week.campaign_target = c_base.campaign_target

  LEFT JOIN campaigns_month ca_month
         ON ca_month.date_month = c_base.date_day
        AND ca_month.site = c_base.site
        AND ca_month.content_source = c_base.content_source
        AND ca_month.advertiser_category = c_base.advertiser_category
        AND ca_month.campaign_target = c_base.campaign_target

  WHERE COALESCE(
    ca_day.total_campaigns_day,
    ca_week.total_campaigns_week,
    ca_month.total_campaigns_month
  ) IS NOT NULL
),

# DISPLAY CAMPAIGNS BUDGET
display_campaigns_budget AS (
  # each campaign_id has only one target_goal
  # for each campaign_id the last budget of the day, week, month must be chosen
  WITH campaigns_budget_day_aux AS (
    SELECT
      DATE(ds) date_day,
      site,
      JSON_EXTRACT_SCALAR(event_data, '$.campaign_id') campaign_id,
      COALESCE(adv.advertiser_category, 'n/a') advertiser_category,
      JSON_EXTRACT_SCALAR(event_data, '$.bid.target_goal') campaign_target,
      ARRAY_AGG(SAFE_CAST(JSON_EXTRACT_SCALAR(ads.event_data, '$.budget.campaign_total') AS FLOAT64) IGNORE NULLS ORDER BY server_timestamp DESC)[OFFSET(0)] campaign_budget_day
    FROM `meli-bi-data.MELIDATA.ADVERTISING` ads

    LEFT JOIN advertiser_categories adv
          ON adv.advertiser_id = JSON_EXTRACT_SCALAR(ads.event_data, '$.advertiser_id')

    WHERE ds BETWEEN date_from AND date_to
      AND site IN UNNEST(sites)
      AND event = 'display_bid_win'

    GROUP BY 1,2,3,4,5
  ),
  campaigns_budget_week_aux AS (
    SELECT
      DATE_TRUNC(date_day, ISOWEEK) date_week,
      site,
      campaign_id,
      advertiser_category,
      campaign_target,
      ARRAY_AGG(campaign_budget_day IGNORE NULLS ORDER BY date_day DESC)[OFFSET(0)] campaign_budget_week
    FROM campaigns_budget_day_aux
    GROUP BY 1,2,3,4,5
  ),
  campaigns_budget_month_aux AS (
    SELECT
      DATE_TRUNC(date_day, MONTH) date_month,
      site,
      campaign_id,
      advertiser_category,
      campaign_target,
      ARRAY_AGG(campaign_budget_day IGNORE NULLS ORDER BY date_day DESC)[OFFSET(0)] campaign_budget_month
    FROM campaigns_budget_day_aux
    GROUP BY 1,2,3,4,5
  ),
  campaigns_budget_day AS (
    SELECT
      date_day,
      site,
      advertiser_category,
      campaign_target,
      SUM(campaign_budget_day) campaign_budget_day
    FROM campaigns_budget_day_aux
    GROUP BY 1,2,3,4
  ),
  campaigns_budget_week AS (
    SELECT
      date_week,
      site,
      advertiser_category,
      campaign_target,
      SUM(campaign_budget_week) campaign_budget_week
    FROM campaigns_budget_week_aux
    GROUP BY 1,2,3,4
  ),
  campaigns_budget_month AS (
    SELECT
      date_month,
      site,
      advertiser_category,
      campaign_target,
      SUM(campaign_budget_month) campaign_budget_month
    FROM campaigns_budget_month_aux
    GROUP BY 1,2,3,4
  ),
  campaigns_budget_base AS (
    WITH days AS (
      SELECT date_day
      FROM UNNEST(GENERATE_DATE_ARRAY(date_from, date_to, INTERVAL 1 DAY)) date_day
    ),
    sites AS (
        SELECT site
        FROM UNNEST(sites) site
    ),
    advertiser_categories AS (
        SELECT DISTINCT category advertiser_category
        FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.advertiser`

        UNION DISTINCT
        SELECT 'n/a' advertiser_category
    ),
    campaign_targets AS (
        SELECT campaign_target
        FROM UNNEST(campaign_targets) campaign_target
    )
    SELECT
      date_day,
      site,
      advertiser_category,
      campaign_target
    FROM days, sites, advertiser_categories, campaign_targets
  )
  SELECT
    cb_base.date_day,
    cb_base.site,
    'n/a' content_source,
    'n/a' priority,
    cb_base.advertiser_category,
    'n/a' placement,
    cb_base.campaign_target,
    COALESCE(cb_day.campaign_budget_day, 0) campaign_budget_day_lc,
    COALESCE(cb_day.campaign_budget_day / cur.CCO_TC_VALUE, 0) campaign_budget_day_usd,
    COALESCE(cb_week.campaign_budget_week, 0) campaign_budget_week_lc,
    COALESCE(cb_week.campaign_budget_week / cur.CCO_TC_VALUE, 0) campaign_budget_week_usd,
    COALESCE(cb_month.campaign_budget_month, 0) campaign_budget_month_lc,
    COALESCE(cb_month.campaign_budget_month / cur.CCO_TC_VALUE, 0) campaign_budget_month_usd
  FROM campaigns_budget_base cb_base

  LEFT JOIN `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` cur
         ON cur.TIM_DAY_TC = cb_base.date_day
        AND TRIM(cur.SIT_SITE_ID) = cb_base.site
        AND cur.CCO_TO_CURRENCY_ID IN ('COP','ARS','BRL','CLP','MXN','UYU','PEN')

  LEFT JOIN campaigns_budget_day cb_day 
         ON cb_day.date_day = cb_base.date_day
        AND cb_day.site = cb_base.site
        AND cb_day.advertiser_category = cb_base.advertiser_category
        AND cb_day.campaign_target = cb_base.campaign_target

  LEFT JOIN campaigns_budget_week cb_week 
         ON cb_week.date_week = cb_base.date_day
        AND cb_week.site = cb_base.site
        AND cb_week.advertiser_category = cb_base.advertiser_category
        AND cb_week.campaign_target = cb_base.campaign_target

  LEFT JOIN campaigns_budget_month cb_month 
         ON cb_month.date_month = cb_base.date_day
        AND cb_month.site = cb_base.site
        AND cb_month.advertiser_category = cb_base.advertiser_category
        AND cb_month.campaign_target = cb_base.campaign_target

  WHERE COALESCE(
    cb_day.campaign_budget_day,
    cb_week.campaign_budget_week,
    cb_month.campaign_budget_month
  ) IS NOT NULL
)

## MAIN QUERY (44k rows)
SELECT

  # BASE TABLE
  bt.date_day,
  bt.site,
  bt.content_source,
  bt.priority,
  bt.advertiser_category,
  bt.placement,
  bt.campaign_target,

  # DISPLAY PRINTS
  dp.total_impressions,
  dp.impression_cost_lc,
  dp.impression_cost_usd,

  # DISPLAY PRINTS EMPTY
  dep.total_empty_impressions,

  # DISPLAY CLICKS
  dc.clicks,

  # DISPLAY ORDERS BY ATTRIBUTION DATE
  daoa.orders_att,
  daoa.orders_print_att,
  daoa.orders_click_att,
  daoa.orders_total_amount_lc_att,
  daoa.orders_total_amount_usd_att,

  # DISPLAY ORDERS BY CONVERTION DATE
  daoc.orders_cre,
  daoc.orders_print_cre,
  daoc.orders_click_cre,
  daoc.orders_total_amount_lc_cre,
  daoc.orders_total_amount_usd_cre,

  # DISPLAY ADVERTISERS
  da.total_advertisers_day,
  da.total_advertisers_week,
  da.total_advertisers_month,

  # DISPLAY CAMPAIGNS
  dca.total_campaigns_day,
  dca.total_campaigns_week,
  dca.total_campaigns_month,

  # DISPLAY CAMPAIGNS BUDGET
  dcb.campaign_budget_day_lc,
  dcb.campaign_budget_day_usd,
  dcb.campaign_budget_week_lc,
  dcb.campaign_budget_week_usd,
  dcb.campaign_budget_month_lc,
  dcb.campaign_budget_month_usd  

FROM base_table bt

LEFT JOIN display_prints dp
       ON dp.date_day = bt.date_day
      AND dp.site = bt.site
      AND dp.content_source = bt.content_source
      AND dp.priority = bt.priority
      AND dp.advertiser_category = bt.advertiser_category
      AND dp.placement = bt.placement
      AND dp.campaign_target = bt.campaign_target

LEFT JOIN display_empty_prints dep
       ON dep.date_day = bt.date_day
      AND dep.site = bt.site
      AND dep.content_source = bt.content_source
      AND dep.priority = bt.priority
      AND dep.advertiser_category = bt.advertiser_category
      AND dep.placement = bt.placement
      AND dep.campaign_target = bt.campaign_target

LEFT JOIN display_clicks dc
       ON dc.date_day = bt.date_day
      AND dc.site = bt.site
      AND dc.content_source = bt.content_source
      AND dc.priority = bt.priority
      AND dc.advertiser_category = bt.advertiser_category
      AND dc.placement = bt.placement
      AND dc.campaign_target = bt.campaign_target

LEFT JOIN display_attribution_orders_att daoa
       ON daoa.date_day = bt.date_day
      AND daoa.site = bt.site
      AND daoa.content_source = bt.content_source
      AND daoa.priority = bt.priority
      AND daoa.advertiser_category = bt.advertiser_category
      AND daoa.placement = bt.placement
      AND daoa.campaign_target = bt.campaign_target

LEFT JOIN display_attribution_orders_cre daoc
       ON daoc.date_day = bt.date_day
      AND daoc.site = bt.site
      AND daoc.content_source = bt.content_source
      AND daoc.priority = bt.priority
      AND daoc.advertiser_category = bt.advertiser_category
      AND daoc.placement = bt.placement
      AND daoc.campaign_target = bt.campaign_target

LEFT JOIN display_advertisers da
       ON da.date_day = bt.date_day
      AND da.site = bt.site
      AND da.content_source = bt.content_source
      AND da.priority = bt.priority
      AND da.advertiser_category = bt.advertiser_category
      AND da.placement = bt.placement
      AND da.campaign_target = bt.campaign_target

LEFT JOIN display_campaigns dca
       ON dca.date_day = bt.date_day
      AND dca.site = bt.site
      AND dca.content_source = bt.content_source
      AND dca.priority = bt.priority
      AND dca.advertiser_category = bt.advertiser_category
      AND dca.placement = bt.placement
      AND dca.campaign_target = bt.campaign_target

LEFT JOIN display_campaigns_budget dcb
       ON dcb.date_day = bt.date_day
      AND dcb.site = bt.site
      AND dcb.content_source = bt.content_source
      AND dcb.priority = bt.priority
      AND dcb.advertiser_category = bt.advertiser_category
      AND dcb.placement = bt.placement
      AND dcb.campaign_target = bt.campaign_target

WHERE COALESCE(
  dp.total_impressions,
  dp.impression_cost_lc,
  dp.impression_cost_usd,
  dep.total_empty_impressions,
  dc.clicks,
  daoa.orders_att,
  daoa.orders_print_att,
  daoa.orders_click_att,
  daoa.orders_total_amount_lc_att,
  daoa.orders_total_amount_usd_att,
  daoc.orders_cre,
  daoc.orders_print_cre,
  daoc.orders_click_cre,
  daoc.orders_total_amount_lc_cre,
  daoc.orders_total_amount_usd_cre,
  da.total_advertisers_day,
  da.total_advertisers_week,
  da.total_advertisers_month,
  dca.total_campaigns_day,
  dca.total_campaigns_week,
  dca.total_campaigns_month,
  dcb.campaign_budget_day_lc,
  dcb.campaign_budget_day_usd,
  dcb.campaign_budget_week_lc,
  dcb.campaign_budget_week_usd,
  dcb.campaign_budget_month_lc,
  dcb.campaign_budget_month_usd 
) IS NOT NULL