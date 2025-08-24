
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select craft
from `space_db`.`people_aggregated`
where craft is null



  
  
    ) dbt_internal_test