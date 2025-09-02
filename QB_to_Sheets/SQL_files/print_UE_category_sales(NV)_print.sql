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
    order by 1 
