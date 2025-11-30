SELECT 
    id,
    title,
    price,
    created_at
FROM products
WHERE price BETWEEN 30 AND 50
ORDER BY created_at DESC;
