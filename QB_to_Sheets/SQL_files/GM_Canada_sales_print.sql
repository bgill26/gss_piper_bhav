SELECT
          d.country_name as country_name
        , city_name
        , COALESCE(merchant_type, 'UNKNOWN') AS business_type
        , CASE WHEN uber_merchant_business_segment LIKE '%SMB%' THEN 'SMB' ELSE 'Enterprise' END AS segment
        , brand as brand_name
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
        , count(workflow_uuid) AS brand_orders
        , count(store_uuid) AS total_stores
        , count(store_uuid) AS total_store
        , SUM(units_sold) AS total_units_sold
        , SUM(sales_usd) AS total_sales
        , DATE_TRUNC('month', DATE(datestr)) AS month

FROM gss_cpg.generalmills_trns d
LEFT JOIN dwh.dim_city dc
    ON d.city_id = dc.city_id

WHERE datestr BETWEEN CAST('2024-01-01' AS VARCHAR) AND CAST(DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' DAY AS VARCHAR)
AND cpg_cat_country_partition in ('GM_FOOD_N_SNACKS_CA')
AND brand in ('Betty Crocker','Nature Valley Bars','Nature Valley Granola','Jolly Rancher','Mott''s Fruitsations','Cheerios','Chex','Lucky Charms','Lucky Charms Cereal','Nesquik Cereal','Honey Nut Clusters','Oatmeal Crisp','GM Original Cereal','Reese''s Puffs','Reese''s Puffs Cereal','Liberté','Yoplait','Yoplait Minigo','Yoplait Tubes','Yoplait Yop','Nature Valley','Cinnamon Toast Crunch')

GROUP BY 1, 2, 3, 4, 5, 6, DATE_TRUNC('month', DATE(datestr))
ORDER BY DATE_TRUNC('month', DATE(datestr)) 
