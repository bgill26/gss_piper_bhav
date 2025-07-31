SELECT
    DATE_TRUNC('month', DATE(datestr)) AS order_month,
    business_type,
    CASE
        WHEN category_l3 IN ('NUT_BUTTERS') THEN 'Dips and Spreads'
        WHEN category_l3= 'CANDY_AND_CHOCOLATE' THEN 'Candy and Chocolate'
        WHEN category_l3 = 'COOKIES_AND_SWEET_BISCUITS' THEN 'Cookies and Sweet Biscuits'
        WHEN category_l3 = 'FROZEN_DESSERTS_AND_TOPPINGS' THEN 'Frozen Desserts and toppings'
        ELSE category_l3
    END AS category,
    SUM(total_quantity) AS units_sold,
    SUM(total_sales_usd) AS total_sales
FROM grdw.agg_gr_order_ads_detail
WHERE DATE(datestr) BETWEEN DATE('2024-01-01') AND DATE('2025-06-30')
    AND country_name IN ('United States')
    AND category_l3 IN ('CANDY_AND_CHOCOLATE', 'COOKIES_AND_SWEET_BISCUITS', 'NUT_BUTTERS', 'FROZEN_DESSERTS_AND_TOPPINGS')
GROUP BY 1, 2, 3
ORDER BY order_month DESC, total_sales DESC;
