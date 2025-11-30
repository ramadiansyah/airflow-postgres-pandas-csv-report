WITH ranked_products AS (
  SELECT
    id,
    created_at,
    title,
    category,
    vendor,
    ROW_NUMBER() OVER (PARTITION BY title ORDER BY created_at) AS rn,
    COUNT(*) OVER (PARTITION BY title) AS title_count
  FROM products
)
SELECT
  id,
  created_at,
  title,
  category,
  vendor 
FROM ranked_products
WHERE title_count > 1
ORDER BY title, created_at;
