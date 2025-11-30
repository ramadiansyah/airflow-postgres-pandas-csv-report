WITH order_summary AS (
    SELECT 
        p.category AS product_category,
        SUM(o.total) AS total_sales
    FROM orders o
    JOIN products p ON o.product_id = p.id
    GROUP BY p.category
)
SELECT 
    product_category,
    total_sales
FROM order_summary
ORDER BY total_sales DESC;

