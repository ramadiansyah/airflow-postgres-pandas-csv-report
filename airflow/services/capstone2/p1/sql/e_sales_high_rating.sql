WITH high_rating_orders AS (
    SELECT 
        p.title AS product_title,
        SUM(o.total) AS total_sales
    FROM orders o
    JOIN products p ON o.product_id = p.id
    WHERE p.rating >= 4
    GROUP BY p.title
)
SELECT 
    product_title,
    total_sales
FROM high_rating_orders
ORDER BY total_sales DESC;
