-- Example SQL Query
SELECT DISTINCT
    a.i,
    f1.Score,
    f1.[Grantee],
    f1.[Name],
    f1.[Hearing_Type],
    a.*
FROM [GIVE].[dbo].[scrape_spy_midnight] a
INNER JOIN (
    SELECT [key], MAX(max_row) AS max_row
    FROM (
        SELECT [key], ROW_NUMBER() OVER (PARTITION BY [key] ORDER BY sale_date) AS max_row
        FROM [GIVE].[dbo].[scrape_spy_midnight]
    ) t2
    GROUP BY [key]
) t1 ON t1.[key] = a.[key]
INNER JOIN [dbo].[fuzzywuzzy_matches_with_links] f1 
    ON f1.[spy_key] = a.[key] 
    AND f1.[Grantee] = a.grantee
WHERE a.grantee NOT LIKE '%LLC%'