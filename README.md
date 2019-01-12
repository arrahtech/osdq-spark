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

In order to get started we need to understand the JSON schema. They are simple to understand and easy to use.
Please refer to [Understanding the JSON elements](/understanding-json-element) in our wiki for details.

Once you understood the json elements, you can refer to our [wiki](/how-to-convert-sql-to-json) to write JSON for respective SQL 


## Things to add
 - Add trim while loading ( optional )
 - Append dataframe name while loading  (optional)
 - Select block from file with necessary filtering
 - Enhance proprietary IF statements  (AND, OR , is Null , IsNot null, contain,regex )
 - Use derived columns on the fly for Join condition


## Architecture

![jsonFrameworkArch2](/uploads/06ecbaa44cd2a5828758ff72f53a975f/jsonFrameworkArch2.png)


![jsonFrameworkArch3](/uploads/e484466cd2977c63b350e313305a58db/jsonFrameworkArch3.png)

