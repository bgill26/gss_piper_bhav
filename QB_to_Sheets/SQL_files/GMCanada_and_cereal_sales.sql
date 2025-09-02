WITH gm AS (
    SELECT
         DATE_TRUNC('month', DATE(datestr)) AS month
         , CASE
                        WHEN brand IN ('Betty Crocker','Jolly Rancher','Mott''s Fruitsations') THEN 'Fruit Snacks'
                WHEN brand IN ('Cheerios','Chex','Lucky Charms','Nesquik Cereal','Honey Nut Clusters','Oatmeal Crisp','GM Original Cereal','Reese''s Puffs','Reese''s Puffs Cereal') THEN 'Cereal'
                WHEN brand IN ('Liberté','Yoplait') THEN 'Yogurt'
                WHEN brand IN ('Nature Valley','Nature Valley Bars','Nature Valley Granola') THEN 'Bars'
                WHEN brand LIKE '%Cinnamon Toast Crunch%' THEN
            CASE
                WHEN LOWER(item_name) LIKE '%bar%' OR LOWER(item_name) LIKE '%bars%' THEN 'Bars'
                    ELSE 'Cereal'
                    END
            END AS category
        , SUM(sales_usd) AS gm_sales
    FROM gss_cpg.generalmills_trns
    WHERE year(DATE(datestr)) IN (2024, 2025)
    AND brand in ('Nature Valley Bars','Nature Valley Granola','Betty Crocker','Jolly Rancher','Mott''s Fruitsations','Cheerios','Chex','Lucky Charms','Nesquik Cereal','Honey Nut Clusters','Oatmeal Crisp','GM Original Cereal','Reese''s Puffs','Reese''s Puffs Cereal','Liberté','Yoplait','Nature Valley','Cinnamon Toast Crunch')
    AND cpg_cat_country_partition in ('GM_FOOD_N_SNACKS_CA')

    GROUP BY DATE_TRUNC('month', DATE(datestr)), 2
),

ue_data AS (
    SELECT
        DATE_TRUNC('month', DATE(processed_date)) AS month,
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
    AND year(processed_date) IN (2024, 2025)  -- Consider only 2024 and 2025
    GROUP BY 1, 2
)

SELECT
    a.month
    , gm_sales
    , ue_sales AS category_sales
    , CAST(ROUND((gm_sales /ue_sales) * 100, 2) AS VARCHAR) || '%' AS category_share
FROM gm a JOIN ue_data b ON a.month = b.month
WHERE a.category IN ('Cereal') AND b.categories IN ('Cereal')
ORDER BY 1
