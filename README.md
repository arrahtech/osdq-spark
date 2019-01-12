# OSDQ Spark

__Release Date: 02-Nov-2018__

__Version: 0.0.1__

__Contact us SUPPORT@ARRAHTECH.COM__

OSDQ Spark is JSON based data processing framework which converts JSON into Apache Spark code and runs it on a spark cluster. This makes it easy for anyone to 
utilize the power of ![Apache Spark](https://spark.apache.org/) withtout knowing much about Spark SQL. 

* How it will help you ? *

1. You do not need spark developers to write data processing code
2. It will enhance the development time
3. Traditional SQL based data processing can be easily changed to JSON
4. Data Processing and Data Storage can be combined
5. Easy to wrap up code and run on cloud

## Getting Started

In order to get started we need to understand the JSON schema. They are simple to understand and use. Nest sub-section describes the same.

### [Understanding the JSON elements](/understanding-json-element)


### How to convert SQL to JSON using osDQ-Spark

- Add datasource for each original tables. Derived tables can be build during data processing phase
- Whichever columns you need you can put in selectedColumns -  [] means all or it can be comma separated
- Joins and Filters can be added in JSON transformation
- Derivative columns are not supported in join conditions to first add columns with derivatives then join
- selectExpr block can be from File. Similarly column header can be taken from file. This files can be multiline. It has a predefined format

> FROM (select * from schema.tablename WHERE FCUR <> 'EUR') tablename    

__Equivalent JSON__

```json
"datasources": [
    {
      "name": "tablename",
      "location":"/path/to/tablename.csv",
      "locationType": "static",
      "format": "csv",
      "selectedColumns": []
    }
```
OR (if it is database)

```json
{
  "name": "tablename",
  "format": "jdbc", 
  "jdbcparam":"url,jdbc:mysql://localhost:3306/mysql,driver,com.mysql.jdbc.Driver,user,root,password,root,dbtable,(select * from schema.tablename WHERE FCUR <> 'EUR') AS T,partitionColumn,parent_category_id,lowerBound,0,upperBound,10000,numPartitions,10",
  "selectedColumns": []
}
```

We have several other examples of different kind or queries, please refer our [wiki](/sample-sql-to-json) page for details.


### Things to add
 - Add trim while loading ( optional )
 - Append dataframe name while loading  (optional)
 - Select block from file with necessary filtering
 - Enhance proprietary IF statements  (AND, OR , is Null , IsNot null, contain,regex )
 - Use derived columns on the fly for Join condition


## Architecture

![jsonFrameworkArch2](/uploads/06ecbaa44cd2a5828758ff72f53a975f/jsonFrameworkArch2.png)


![jsonFrameworkArch3](/uploads/e484466cd2977c63b350e313305a58db/jsonFrameworkArch3.png)

