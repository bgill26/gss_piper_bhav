WITH gm AS (
    SELECT
         year(DATE(datestr)) AS year
         , CASE
                        WHEN brand IN ('Betty Crocker','Jolly Rancher','Mott''s Fruitsations') THEN 'Fruit Snacks'
                WHEN brand IN ('Cheerios','Chex','Lucky Charms','Lucky Charms Cereal','Nesquik Cereal','Honey Nut Clusters','Oatmeal Crisp','GM Original Cereal','Reese''s Puffs','Reese''s Puffs Cereal') THEN 'Cereal'
                WHEN brand IN ('Liberté','Yoplait','Yoplait Minigo','Yoplait Tubes','Yoplait Yop') THEN 'Yogurt'
                WHEN brand IN ('Nature Valley','Nature Valley Bars','Nature Valley Granola') THEN 'Bars'
                WHEN brand LIKE '%Cinnamon Toast Crunch%' THEN
            CASE
                WHEN LOWER(item_name) LIKE '%bar%' OR LOWER(item_name) LIKE '%bars%' THEN 'Bars'
                    ELSE 'Cereal'
                    END
            END AS category
        , SUM(sales_usd) AS gm_sales
    FROM gss_cpg.generalmills_trns
    WHERE CAST(date_format(DATE(datestr), '%m') AS INTEGER) BETWEEN 1 AND CAST(date_format(current_date, '%m') AS INTEGER) - 1
    AND year(DATE(datestr)) IN (2024, 2025)
    AND brand in ('Betty Crocker','Nature Valley Bars','Nature Valley Granola','Jolly Rancher','Mott''s Fruitsations','Cheerios','Chex','Lucky Charms','Lucky Charms Cereal','Nesquik Cereal','Honey Nut Clusters','Oatmeal Crisp','GM Original Cereal','Reese''s Puffs','Reese''s Puffs Cereal','Liberté','Yoplait','Yoplait Minigo','Yoplait Tubes','Yoplait Yop','Nature Valley','Cinnamon Toast Crunch')
    AND cpg_cat_country_partition in ('GM_FOOD_N_SNACKS_CA')

    GROUP BY year(DATE(datestr)), 2
),

ue_data AS (
    SELECT
        year(DATE_TRUNC('month', DATE(processed_date))) AS year,
        CASE
            WHEN sub_category IN ('Cereal & Granola', 'Cereal, Oatmeal & Granola', 'Oats, Grits & Hot Cereal') THEN 'Cereal'
            WHEN sub_category = 'Bars' THEN 'Bars'
            WHEN sub_category = 'Fruit Snacks' THEN 'Fruit Snacks'
            WHEN sub_category = 'Yogurt' THEN 'Yogurt'
        END AS categories,
        SUM(sales) AS ue_sales
    FROM kirby_external_data.cpg_category_sales_global_daily_snapshot
    WHERE country_name = 'Canada'
    AND sub_category IN ('Cereal & Granola', 'Cereal, Oatmeal & Granola', 'Oats, Grits & Hot Cereal', 'Bars', 'Fruit Snacks', 'Yogurt')
    AND month(processed_date) BETWEEN 1 AND month(current_date) - 1
    AND year(processed_date) IN (2024, 2025)  -- Consider only 2024 and 2025
    GROUP BY 1, 2
)

SELECT
    CASE
        -- For 2024 row, show period from Jan 23 to Aug 23 (or current month in 2024)
        WHEN gm.year = 2024 THEN CONCAT('Jan 24 - ', date_format(date_trunc('month', date_add('month', -1, CURRENT_DATE)), '%b 24'))

        -- For 2025 row, show period from Jan 24 to the previous month of 2025
        WHEN gm.year = 2025 THEN CONCAT('Jan 25 - ', date_format(date_trunc('month', date_add('month', -1, CURRENT_DATE)), '%b 25'))
    END AS time_period,
    ROUND(gm.gm_sales, 2) AS gm_sales,
    ROUND(ue.ue_sales, 2) AS ue_sales,
    CAST(ROUND((gm.gm_sales / ue.ue_sales) * 100, 2) AS VARCHAR) || '%' AS category_share
FROM gm
JOIN ue_data ue
    ON gm.year = ue.year
    AND ue.categories = 'Yogurt' -- Filter by category for both years
    AND gm.category = 'Yogurt'
WHERE gm.year IN (2024, 2025);
