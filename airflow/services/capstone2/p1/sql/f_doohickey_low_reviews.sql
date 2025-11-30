WITH doohickey_reviews AS (
    SELECT 
        r.created_at,
        r.body,
        r.rating
    FROM reviews r
    JOIN products p ON r.product_id = p.id
    WHERE p.category = 'Doohickey'
      AND r.rating <= 3
)
SELECT 
    created_at,
    body,
    rating
FROM doohickey_reviews
ORDER BY created_at DESC;
