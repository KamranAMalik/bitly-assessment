Assume you've been working on breaking up a query/model that hangs due to intensive use of resources. It is a model that selects from a source and tries to create a dimension table, but the scale of the data scanned is too much to do in one query. How would you make use of the suggested dbt project structure to break it up?



In order to make this table/dbt model run more efficiently, I would do the following: 
a. run this model incrementally in order to not do a full table scan and update all records and just records within the last few days which is defined by the business. 
b. break the query up into different intermediaries. for example, if necessary I would break up the model and add certain dimensions to an intermediary layer. 
c. 