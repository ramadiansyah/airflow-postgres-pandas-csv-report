SELECT 
    name,
    email,
    address,
    birth_date
FROM users
WHERE CAST(SUBSTRING(birth_date FROM 1 FOR 4) AS INTEGER) > 1997;
