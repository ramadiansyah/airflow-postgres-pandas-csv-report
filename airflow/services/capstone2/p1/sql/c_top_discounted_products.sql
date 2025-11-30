SELECT 
    p.title AS product_title,
    COUNT(o.id) AS total_discounted_orders
FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.discount > 0
GROUP BY p.title
ORDER BY total_discounted_orders DESC
LIMIT 10;

