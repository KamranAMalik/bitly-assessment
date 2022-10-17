Assume you have a source table that is refreshed daily with new data for the prior day, for example, product usage data. Data older than the previous day is not changed. Assume you are creating a process to bring this data into another table. Explain how you would create a process that incrementally brings the data to the target table (that is, the process should only bring data not in the target table). How would you ensure the process is self-correcting, that is, if the process fails one day, it corrects itself the next time it runs by bringing data for the days missing in the target table? How would you test the data in both tables matches?


For this process, I would run the data incrementally. dbt has a incremental strategy and to implement this and to make sure it is a self-correcting process I would do the following. This would ensure that it would grab all records from the source_table by checking the final table and getting the max(last_update_timestamp) and getting all of the records from the source table which are greater than that value.

with source as (

    select * from source_table 
    {%- if is_incremental() %}
    where last_update_timestamp > (select max(last_update_timestamp)) from {{ this }})
    {%- endif %}
)