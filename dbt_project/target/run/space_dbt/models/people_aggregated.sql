
  
    
    
    
        
         
          

        insert into `space_db`.`people_aggregated`
        ("craft", "astronaut_count", "last_updated", "rank")

SELECT 
    craft, 
    COUNT(name) AS astronaut_count,
    MAX(_inserted_at) AS last_updated,
    ROW_NUMBER() OVER (ORDER BY COUNT(name) DESC) AS rank
FROM `space_db`.`people`  # Изменено на 'raw'
GROUP BY craft
HAVING astronaut_count > 0
  