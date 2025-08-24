{{ config(materialized='table') }}

SELECT 
    craft, 
    COUNT(name) AS astronaut_count,
    MAX(_inserted_at) AS last_updated,
    ROW_NUMBER() OVER (ORDER BY COUNT(name) DESC) AS rank
FROM {{ source('raw', 'people') }}   
GROUP BY craft
HAVING astronaut_count > 0