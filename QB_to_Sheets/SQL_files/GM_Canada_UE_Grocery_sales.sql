WITH gm AS (
    SELECT
         DATE_TRUNC('month', DATE(datestr)) AS month
        , SUM(sales_usd) AS gm_sales
    FROM gss_cpg.generalmills_trns
    WHERE
     year(DATE(datestr)) IN (2024, 2025)
    -- AND brand in ('Betty Crocker','Nature Valley Bars','Nature Valley Granola','Jolly Rancher','Mott''s Fruitsations','Cheerios','Chex','Lucky Charms','Lucky Charms Cereal','Nesquik Cereal','Honey Nut Clusters','Oatmeal Crisp','GM Original Cereal','Reese''s Puffs','Reese''s Puffs Cereal','Liberté','Yoplait','Yoplait Minigo','Yoplait Tubes','Yoplait Yop','Nature Valley','Cinnamon Toast Crunch')
    AND cpg_cat_country_partition in ('GM_FOOD_N_SNACKS_CA')
    -- AND merchant_type in ('GROCERY')

    GROUP BY DATE_TRUNC('month', DATE(datestr))
),

ue_data AS (
    SELECT
        DATE_TRUNC('month', DATE(processed_date)) AS month,
        SUM(sales) AS ue_sales
    FROM kirby_external_data.cpg_category_sales_global_daily_snapshot
    WHERE country_name = 'Canada'
    -- AND sub_category IN ('Cereal & Granola', 'Cereal, Oatmeal & Granola', 'Oats, Grits & Hot Cereal', 'Bars', 'Fruit Snacks', 'Yogurt')
    AND year(processed_date) IN (2024, 2025)  -- Consider only 2024 and 2025
    and business_type in ('GROCERY')
    GROUP BY 1
)

SELECT
    a.month
    , ROUND(SUM(gm_sales)) AS gm_sales
    , ROUND(SUM(ue_sales)) AS ue_sales
    , CAST(ROUND((SUM(gm_sales) / SUM(ue_sales)) * 100, 2) AS VARCHAR) || '%' AS business_type_share

FROM gm a JOIN ue_data b ON a.month = b.month
GROUP BY 1
ORDER BY 1
