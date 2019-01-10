**Framework Readme**

**Date 02-Nov-2018**

**V.0.0.1**

## Why Framework is needed

Framework is JSON based data processing module which converts JSON into Apache Spark code and runs on spark cluster.

1. How it will help you ?
2. You do not need spark developers to write data processing code
3. It will enhance the development time
4. Traditional SQL based data processing can be easily changed to JSON
5. Data Processing and Data Storage can be combined
6. Easy to wrap up code and run on cloud

### Architecture

TBD
![Architecture](./jsonFrameworkArch2.svg)

TBD
![Architecture](https://gitlab.com/arun-y/osdq-spark-framework/raw/master/jsonFrameworkArch3.svg)


## Using Framework


### how to convert SQL to JSON using osDQ-Spark

- Add datasource for each original tables. Derived tables can be build during data processing phase
- Whichever columns you need you can put in selectedColumns -  [] means all or it can be comma separated
- Joins and Filters can be added in JSON transformation
- Derivative columns are not supported in join conditions to first add columns with derivatives then join
- selectExpr block can be from File. Similarly column header can be taken from file. This files can be multiline. It has a predefined format


> FROM (select * from lpfg_core.tablename WHERE FCUR <> 'EUR'  )tablename    

### Equivalent JSON

```json
"datasources": [
    {
      "name": "tablename",
      "location":"/Users/vsingh007c/Downloads/drive-download-20180718T060717Z-001/cdc/tablename.csv",
      "locationType": "static",
      "format": "csv",
      "selectedColumns": []
    }

OR (if it is database)
    {
      "name": "tablename",
      "format": "jdbc", "jdbcparam":"url,jdbc:mysql://localhost:3306/mysql,driver,com.mysql.jdbc.Driver,user,root,password,root,dbtable,(select * from lpfg_core.tablename WHERE FCUR <> 'EUR') AS T,partitionColumn,parent_category_id,lowerBound,0,upperBound,10000,numPartitions,10",
      "selectedColumns": []
    }

  ]
  ```
  
  
> LEFT OUTER JOIN lpfg_wrk.vendors  vendor   on (regexp_replace(trim(tablename.VENDOR_no),"^0*","")=regexp_replace(trim(vendor.lifnr),"^0*","") and tablename.source_id=vendor.source_id ) 

### Equivalent JSON

```json
"transformations": [
    {
      "name": "vendor",
      "type": "enrichment",
      "source": "vendor",
      "priority": 1,
      "cache": false,
      "conditions": [
      {
          "condition": "renameallcolumns",
          "aggrcondition": ”vendor_"
        },
        {
          "condition": ”replacecolumns",
          "aggrcondition": "vendors_ lifnr :regexp_replace(trim(vendor_lifnr),\"^0*\",\”\")"
        }
      ]
    },

    {
      "name": ”tablename",
      "type": "enrichment",
      "source": ”tablename",
      "priority": 2,
      "cache": false,
      "conditions": [
        {
          "condition": "renameallcolumns",
          "aggrcondition": ”tablename_"
        },
        {
          "condition": ”replacecolumns",
          "aggrcondition": " tablename_VENDOR_no :regexp_replace(trim(tablename_VENDOR_no),\"^0*\",\”\")
        }
      ]
    }
    
    ```
    
> New SQL -- LEFT OUTER JOIN lpfg_wrk.vendors vendor on tablename_VENDOR_no=vendor_lifnr and tablename_source_id=vendor_source_id ) 

### Equivalent JSON
 
```json
{
      "name": ”tablename",
      "type": "join",
      "source": ”tablename,vendor",
      "priority": 3,
      "cache": false,
      "conditions": [
        {
          "joinType": "left_outer",
          "leftcolumn": "tablename_VENDOR_no, tablename_source_id ",
          "condition": "equal",
          "rightcolumn": "vendor_lifnr,vendor_source_id",
          "dropcolumn": ""
        }
      ]
    }
```

__Now do a left outer join and update tablename to have those joined columns. Do not drop any columns. Nth column is matched with nth column__


> LEFT OUTER JOIN lpfg_stg.EXCHANGE_RATES  ER_ACT ON (TRIM(ER_ACT.CURRENCY_FROM) = TRIM(FCUR) AND TRIM(ER_ACT.CURRENCY_TO) = 'EUR’ AND ER_ACT.VALID_FROM_DATE = tablename.FROM_DATE AND TRIM(ER_ACT.EXRT_TYPE) = 'M')

### Equivalent JSON

```json
{
      "name": "ER_ACT",
      "type": "filter",
      "source": "EXCHANGE_RATES",
      "priority": 4,
      "cache": false,
      "conditions": [
        {
          "condition": "Expression",
          "value": ["constant",”TRIM(ER_ACT.CURRENCY_TO) = 'EUR’ AND TRIM(ER_ACT.EXRT_TYPE) = 'M')"],
          "datatype": "String"
        }
}
```
__It has combinations of filter conditions and join conditions. So first take filter condition then join__

> New SQL -- LEFT OUTER JOIN  ER_ACT ON (TRIM(ER_ACT.CURRENCY_FROM) = TRIM(FCUR) AND ER_ACT.VALID_FROM_DATE = tablename.FROM_DATE

### Equivalent JSON

```json
{
      "name": ”tablename",
      "type": "join",
      "source": ”tablename,ER_ACT",
      "priority": 5,
      "cache": false,
      "conditions": [
        {
          "joinType": "left_outer",
          "leftcolumn": "tablename_FCUR,tablename_FROM_DATE ",
          "condition": "equal",
          "rightcolumn": "ER_ACT_CURRENCY_FROM,ER_ACT_VALID_FROM_DATE",
          "dropcolumn": ""
        }
      ]
    }
```

> select distinct INVOICECOUNT , tablename.SOURCE_ID ,tablename.SOURCE_REGION ,POSTING_YEAR .... cast(tablename.CONVERSION_FACTOR as double),'EUR’,  …….

###Equivalent JSON

```json
{
      "name": ”tablename-simple",
      "type": "filter",
      "source": ”tablename",
      "priority": 3,
      "cache": false,
      "conditions": [
        {
          "condition": "DROPDUPLICATES",
          "value": ["constant",”tablename_INVOICECOUNTtablename"],
          "datatype": "String"
        },
        {
          "condition": "selectexpr",
          "value": ["constant” “tablename_INVOICECOUNT” , “tablename_SOURCE_ID” “tablename_SOURCE_REGION”,”POSTING_YEAR”, cast(tablename.CONVERSION_FACTOR as double),'EUR’,],
          "datatype": "String"
        }
      ]
    }
```

- One of easiest and quickest way is take the select block and put into selectExpr block. However it might have 
- Performance issues as it may need many shuffling inside one dataframe.
- Also if select block has non standard functions ( ANSI SQL)  or UDFs it will not work.  Those columns should be treated separately.
- Other option is take additive select columns together and conditional columns ( case, if etc) together


> case when (STOCKING_UOM <> PURCHASING_UOM  and purchase_uom_disp.conversion_factor is not null) then ((RECEIPT_QUANTITY/cast(tablename.CONVERSION_FACTOR as double))*(cast(purchase_uom_disp.conversion_factor as double)))
>	when ((STOCKING_UOM = PURCHASING_UOM  and purchase_uom_disp.conversion_factor is not null) AND PURCHASING_UOM <> purchase_uom_disp.uom_display and tablename.source_id=purchase_uom_disp.source_id) then 
>		(RECEIPT_QUANTITY*(cast(purchase_uom_disp.conversion_factor as double))) else RECEIPT_QUANTITY END as RECEIPT_QUANTITY_REPORTUOM, 
> {
>      "name": ”tablename",
>      "type": "enrichment",
>      "source": ”tablename",
>      "priority": 6,
>      "cache": false,
>      "conditions": [
>        {
>          "condition": "addcolumns",
>          "aggrcondition": "RECEIPT_QUANTITY_REPORTUOM : case when (STOCKING_UOM <> PURCHASING_UOM  and purchase_uom_disp_conversion_factor is not null) then ((RECEIPT_QUANTITY/cast(tablename_CONVERSION_FACTOR as double))*(cast(purchase_uom_disp.conversion_factor as double))) when ((STOCKING_UOM = PURCHASING_UOM  and purchase_uom_disp.conversion_factor is not null) AND PURCHASING_UOM <> purchase_uom_disp.uom_display and tablename.source_id=purchase_uom_disp.source_id) then 
>(RECEIPT_QUANTITY*(cast(purchase_uom_disp.conversion_factor as double))) else RECEIPT_QUANTITY END “
>        }
>Or can write you own condition using format
>       ]
>    }


> case when (STOCKING_UOM <> PURCHASING_UOM  and purchase_uom_disp.conversion_factor is not null) then ((RECEIPT_QUANTITY/cast(tablename.CONVERSION_FACTOR as double))*(cast(purchase_uom_disp.conversion_factor as double)))
>	when ((STOCKING_UOM = PURCHASING_UOM  and purchase_uom_disp.conversion_factor is not null) AND PURCHASING_UOM <> purchase_uom_disp.uom_display and tablename.source_id=purchase_uom_disp.source_id) then 
>		(RECEIPT_QUANTITY*(cast(purchase_uom_disp.conversion_factor as double))) else RECEIPT_QUANTITY END as RECEIPT_QUANTITY_REPORTUOM, 

###Equivalent JSON

... Or can write you own condition using  format ( Proprietary)

```json
        {
          "condition": "conditionalcolumn",
          "aggrcondition": "RECEIPT_QUANTITY_REPORTUOM : IF (STOCKING_UOM <> PURCHASING_UOM) THEN ((RECEIPT_QUANTITY/cast(tablename_CONVERSION_FACTOR as double))*(cast(purchase_uom_disp.conversion_factor as double))) ELSEIF (STOCKING_UOM = PURCHASING_UOM ) THEN (RECEIPT_QUANTITY*(cast(purchase_uom_disp.conversion_factor as double))) OTHERWISE RECEIPT_QUANTITY "
        },
--- now() as run_date,   System functions
        {
          "condition": "addcolumns",
          "aggrcondition": " run_date :now()"
        }
       ]
    }
```

#### Things to add
 - Add trim while loading ( optional )
 - Append dataframe name while loading  (optional)
 - Select block from file with necessary filtering
 - Enhance proprietary IF statements  (AND, OR , is Null , IsNot null, contain,regex )
 - Use derived columns on the fly for Join condition


### 

This json contains 4 major block - sparkconfig, datasources, transformations and outputs.

sparkconfig is optional and it takes the spark parameter. If this block is not there it will take parameter from spark-submit.

```json
"sparkconfig": 
    {
      "appname": "TransformRunner",
      "master":"local[3]",
      "verbose":"true",
      "noexecutor":"3",
      "nocore":"3",
      "executormemory":"3G",
	"confparam":"spark.speculation,false,spark.hadoop.mapreduce.map.speculative,false,spark.hadoop.mapreduce.reduce.speculative,false"
    }
```
 datasources is must and and an array which takes input files or folder to load as dataframe. At present parquet,csv and text format are support. Multiple input files are loaded.

```json
"datasources": [
    {
      "name": "SampleData",
"location":"/Users/vsingh007c/Downloads/ modified.csv",
      "locationType": "static",
      "format": "csv",
      "selectedColumns": []
},
{
   "name": "ORIG",
      "location":"/Users/vsingh007c/Downloads/parquer_directory",
      "locationType": "static",
      "format": "parquet",
      "selectedColumns": [] // empty means load all columns
    }
]
```

 transformations is top level key which is array type. It also has “conditions” key which is array type. It takes a dataframe and apply transformation on it and save the output as another dataframe. If the name and source is same it will override the same dataframe.


```json
{
      "name": "SampleData", // name of dataframe after transformation
      "type": "sampling",  // which type of transformation is applied
      "source": "SampleData", // input dataframe
      "priority": 1, // first in data pipe line. priority should not be same

      "cache": false, // should the dataframe cached
      "conditions": [ // an array of conditions that needs to be appplied
        {
          "condition": "random", // do random sampling 
          "value": ["0.20"] // sample size 20%
        }
      ]
    }
```

Following “transformations”.type are supported for now
“join”,”filter”,”query”,”aggregate”,”enrichment”,”sampling”,”profile”,”transforamtion” The conditions array values will change to as per type value.


If transformation is “join” type following  are supported

"transformations"."conditions"."joinType" are supported – “inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti”,unionall”,”except”,”intersect”. It uses spark jointype in backend so if apache spark supports more joins it will support more join.

```json
{
      "name": "ORIG-DELTA-JOIN",
      "type": "join",
      "source": "ORIG,DELTA",
      "priority": 1,
      "cache": false,
      "conditions": [

{
          "joinType": "left_outer",
          "leftcolumn": "rename1,rename2,rename3",
          "condition": "not equal",
          "rightcolumn": "rename1,rename2",
          "dropcolumn": "rename2"
        }
      ]
    }
```

"transformations"."conditions"." condition " are supported – “equal”,”not equal”, “less than equal”, ‘greater than equal”

unionAll will merge two dataset based on rows like sql union
except will merge two dataset based on rows like sql except
intersecct will merge two dataset based on rows like sql intersect


b.) If transformation is “aggregate” type following  are supported

"transformations"."conditions"." condition " – “groupby”
"transformations"."conditions"."aggrcondition" – it is key val pair of columns and aggregate type separated by comma. These are aggregate types are supported  - min,max,count,sum,avg

          "aggrcondition": "di_sequence_number,max,col2,min"

"transformations"."conditions"." value " is an array which takes all column on which you want to do groupby in same order separated by “,” (comma)

```json
{
      "name": "Groupdata",
      "type": "aggregate",
      "source": "ORIG-DELTA-JOIN",
      "priority": 2,
      "cache": false,
      "conditions": [
        {
          "condition": "groupby",
          "value": [ "col1","col2","col3","col4"], // groupby on given columns
          "aggrcondition": "col1,max" // take max of column 1
        }
      ]
    },
```

If transformation is “enrichment” type following  are supported
"transformations"."conditions"." condition " – “nullreplace”,”dropcolumns”,”addcolumns”,”renamecolumns”,”reordercolumns”,”repalcecolumns”,”renameallcolumns”

"transformations"."conditions"."aggrcondition" is key value pair of column and replace with value , separated by comma like
"aggrcondition": "testcolumn,1"

It will replace column “testcolumn” null values with “1”.

```json
{
      "name": "Cleandata",
      "type": "enrichment",
      "source": "Groupdata",
      "priority": 3,
      "cache": false,
      "conditions": [
        {
          "condition": "nullreplace",
          "aggrcondition": "testcolumn,1" // replace null from testcolumn with 1
        },
{
          "condition": "reordercolumns",
          "aggrcondition": "rename1,rename2,rename3"
        }
      ]
    }
```



examples for addcolumns and renamecolumns

```json
{
      "name": "SampleData",
      "type": "enrichment",
      "source": "SampleData",
      "priority": 4,
      "cache": false,
      "conditions": [
        {
          "condition": "addcolumns",
          "aggrcondition": "testcolumn1:trim(col10):substrcol1:substr(now(),1,4):newcolumn:concat(testcolumn1,col4)" // here is separator is : as functions might have , as parameters
// It will add column testcolumn1 which is trim of col19, will add column substrcol1 which is substring of function now() 

// it uses sql type function to create new columns from existing columns. If a literal is given it will populate the column with same literals.

        }
      ]
    },
    {
      "name": "SampleData",
      "type": "enrichment",
      "source": "SampleData",
      "priority": 8,
      "cache": false,
      "conditions": [
        {
          "condition": "renamecolumns",
          "aggrcondition": "testcolumn1,rename1,substrcol1,rename2,newcolumn,rename3" // rename testcolumn1 to rename1, substrcol1 column to rename2 and so on
        }
      ]
    },
{
          "condition": "renameallcolumns",
          "aggrcondition": "sample_"
        }
```

If transformation is “filter” type following  are supported

"transformations"."conditions"." condition " – “GREATER”,”LESS”,”EQUALS”,”NOTEQUALS”,”IN”,GREATEREQUAL”,”LESSEQUAL”,”EXPRESSION”,”LIKE”,”RLIKE”,DROPDUPLICATES, SELECTEXPR

```json
{
      "name": "SampleData-Filter",
      "type": "filter",
      "source": "SampleData",
      "priority": 3,
      "cache": false,
      "conditions": [
        {
          "column": "col10", // apply filter on this column
          "condition": "GREATER",
          "value": ["variable","datenow","yyyy"], // use used define function datenow with parameter “YYYY” which output should be applied for filter
          "datatype": "Long"
        },
{
          "column": "col1", // apply filter on this column
          "condition": "GREATER",
          "value": ["constant","10”], //pull all the rows which are greater than 10          "datatype": "Long"
        },
{
          "condition": "Expression",
          "value": ["constant","col5 is null"],
          "datatype": "String"
        },
{
          "condition": "RLIKE",
          "column":"col6",
          "value": ["constant","^N"],
          "datatype": "String"
        },
        {
          "condition": "DROPDUPLICATES",
          "value": ["constant","col6","col10"],
          "datatype": "String"
        },
        {
          "condition": "selectexpr",
          "value": ["constant","col6","col10 as none", "'5'", "*"],
          "datatype": "String"
        }

      ]
    }
```

If transformation is “query” type following  are supported

"transformations"."conditions"." sql " – sql will be fired on the dataframe “name” after registering as table in spark like

```json
{
"name": "STBSQLMETRIC",
      "type": "query",
      "source": "STB-PN-JOIN",
      "priority": 5,
      "cache": false,
      "conditions": [
        {
          "sql": "select Programmer,count(*) from STBSQLMETRIC group by Programmer"
        }
      ]
    }
```

If transformation is “sampling” type following  are supported
“random” and “stratified” 

```json
{
      "name": "StratifiedData",
      "type": "sampling",
      "source": "SampleData",
      "priority": 6,
      "cache": false,
      "conditions": [
        {
          "condition": "stratified",
          "value": ["0.002","I","0.8"],
          "column": "col2" // make stratified sampling based on values of col2, if values if not given then default is 0.002 else use the value given like value “I” should be taken 80%. A default value 0.00 mean only values given in array in taken
        }
      ]
    }
```

If transformation is “transformation” type following  are supported
“udf” and “normalise” 
“zeroscore” and “zscore” are supported for normalized.

```json
{
          "condition":"UDF",
          "funcname":"tolong", // user defined function
          "value": ["LowertestcolumnUDF"], // column input for UDF function
          "column": "longLowertestcolumnUDF", // name of column after applying UDF
          "datatype": "Long" // datatype of output column
        },
        {
          "condition":"normalise",
          "funcname":"zeroscore", // normalization function to be applied
          "value": ["col10"], // input column for normalization
          "column": "normcol10", // output column name after normalization
          "datatype": "Long"
        },
{
          "condition":"normalise",
          "funcname":"zscore",
          "value": ["col10"],
          "column": "zcol10",
          "datatype": "Long"
        },
{
          "condition":"UDF",
          "funcname":"regex_replace",
          "value": ["col6"],
          "column": "regexcolumnUDF",
          "aggrcondition": "N.*,vivek",
          "datatype": "string"
        }
}
```

 Outputs: It saves the dataframe as given location. It is an array so multiple dataframes can be added to be saved.

Following json snipped with save dataframe "UDFData-Filter-newCol" at location “/Users/vsingh007c/Downloads/” inside directory BASE_TABLE in parquet format in append mode.

```json
"outputs": [
    {
      "name": "BASE_TABLE",
      "location": "/Users/vsingh007c/Downloads/",
      "sources": ["UDFData-Filter-newCol"],
      "format": "parquet",
      "savemode": "append",
      "selectedColumns": []
    }
  ]
```
Examples: complete json

```json
{
  "sparkconfig": 
    {
      "appname": "TransformRunner",
      "master":"local[8]",
      "verbose":"false",
      "noexecutor":"3",
      "nocore":"3",
      "executormemory":"3G",
      "confparam":"spark.speculation,false,spark.hadoop.mapreduce.map.speculative,false,spark.hadoop.mapreduce.reduce.speculative,false"
    },
  "datasources": [
    {
      "name": "mat",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/Material/material.csv",
      "locationType": "static",
      "format": "|",
      "selectedColumns": [],
      "header":"false"
    },
    {
      "name": "PPV",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/PVC_OUTPUT/AP-5160.csv",
      "locationType": "static",
      "format": "csv",
      "selectedColumns": [],
      "header":"false"
    },
    {
      "name": "PO",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/PO_VS_SSE_RECONCILE/po_vs_sse.txt",
      "locationType": "static",
      "format": "|",
      "selectedColumns": [],
      "header":"false"
    },
    {
      "name": "SSE",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/SSE/sse.txt",
      "locationType": "static",
      "format": "|",
      "selectedColumns": [],
      "header":"false"
    },
    {
      "name": "ITCST",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/stdcost/stdcost.dat",
      "locationType": "static",
      "format": "|",
      "selectedColumns": [],
      "header":"false"
    },
    {
      "name": "VD",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/VENDOR/vendor.txt",
      "locationType": "static",
      "format": "|",
      "selectedColumns": [],
      "header":"false"
    },
    {
      "name": "PO_SOURCING",
      "location":"/Users/vsingh007c/Downloads/datasample/spark_sample_data_1st_insert/PO/po.txt",
      "locationType": "static",
      "format": "|",
      "selectedColumns": [],
      "header":"false"
    }
  ],
  "transformations": [
    
    {
    "name": "mat",
      "type": "filter",
      "source": "mat",
      "priority": 1,
      "cache": false,
      "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","PPV_ReconcileMaterial.txt"],
          "datatype": "String"
        }
      ]
    },
    {
    "name": "PPV",
      "type": "filter",
      "source": "PPV",
      "priority": 2,
      "cache": false,
      "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","PVC_OUTPUT.txt"],
          "datatype": "String"
        },
        {
          "condition": "expression",
          "value": ["constant","SUBSTRING(cast(period as string),1,4) >= '2015' "],
          "datatype": "Integer"
        }
        
      ]
    },
    {
    "name": "PO",
      "type": "filter",
      "source": "PO",
      "priority": 3,
      "cache": false,
      "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","PO_VS_SSE_RECONCILE.txt"],
          "datatype": "String"
        }
      ]
    },
    {
      "name": "PO",
      "type": "aggregate",
      "source": "PO",
      "priority": 4,
      "cache": false,
      "conditions": [
        {
          "condition": "groupby",
          "value": ["region","PO_PLANT","po_number","po_material_number"],
          "aggrcondition": "min,sse_price,min,sse_currency,max,BPVOLUME,MAX,SOURCING_TYPE"
        }
      ]
    },
    {
      "name": "PPV",
      "type": "join",
      "source": "PPV,mat",
      "priority": 5,
      "cache": false,
      "conditions": [
        {
          "joinType": "inner",
          "leftcolumn": "plant,material",
          "condition": "equal",
          "rightcolumn": "env,MATNR",
          "dropcolumn": "",
          "onconflict":"PPV_,mat_"
        }
      ]
    },
    {
      "name": "PPV",
      "type": "enrichment",
      "source": "PPV",
      "priority": 6,
      "cache": false,
      "conditions": [
        {
          "condition": "addcolumns",
          "aggrcondition": "plantcode:substring(plant, 1, 2)"
        }
      ]
    },
    {
      "name": "PO",
      "type": "enrichment",
      "source": "PO",
      "priority": 7,
      "cache": false,
      "conditions": [
        {
          "condition": "addcolumns",
          "aggrcondition": "plantcode:substring(region, 1, 2)"
        }
      ]
    },
    {
      "name": "PPV",
      "type": "join",
      "source": "PPV,PO",
      "priority": 8,
      "cache": false,
      "conditions": [
        {
          "joinType": "left_outer",
          "leftcolumn": "plantcode,plant,material,ponumber",
          "condition": "equal",
          "rightcolumn": "plantcode,PO_PLANT,po_material_number,po_number",
          "dropcolumn": "",
          "onconflict":"PPV_,PO_"
        }
      ]
    },
    {
    "name": "SSE",
    "type": "filter",
    "source": "SSE",
    "priority": 9,
    "cache": false,
    "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","sseheader.txt"],
          "datatype": "String"
        }
    ]
    },
    {
      "name": "SSE",
      "type": "query",
      "source": "SSE",
      "priority": 10,
      "cache": false,
      "conditions": [
        {
          "condition": "file,ssegroupbysql.txt",
          "sql": ""
        }
      ]
    },
    {
    "name": "ITCST",
      "type": "filter",
      "source": "ITCST",
      "priority": 11,
      "cache": false,
      "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","STDCOST.txt"]
        },
        {
          "condition": "expression",
          "value": ["constant","MATERIAL_COST > 0 "]
        }
      ]
    },
    {
      "name": "PPV",
      "type": "join",
      "source": "PPV,ITCST",
      "priority": 12,
      "cache": false,
      "conditions": [
        {
          "joinType": "left_outer",
          "leftcolumn": "plant,material,period",
          "condition": "equal",
          "rightcolumn": "ITCST_env,MATNR,YEAR",
          "dropcolumn": "",
          "onconflict":"PPV_,ITCST_"
        }
      ]
    },
    {
      "name": "PPV",
      "type": "join",
      "source": "PPV,SSE",
      "priority": 13,
      "cache": false,
      "conditions": [
        {
          "joinType": "left_outer",
          "leftcolumn": "plant,material",
          "condition": "equal",
          "rightcolumn": "SOURCEPLANT,SOURCEMATERIALID",
          "dropcolumn": "",
          "onconflict":"PPV_,SSE_"
        }
      ]
    },
    {
    "name": "VD",
      "type": "filter",
      "source": "VD",
      "priority": 14,
      "cache": false,
      "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","vendors.txt"]
        }
      ]
    },
    {
      "name": "PPV",
      "type": "join",
      "source": "PPV,VD",
      "priority": 15,
      "cache": false,
      "conditions": [
        {
          "joinType": "left_outer",
          "leftcolumn": "po_vendornumber,PPV_SOURCE_ID",
          "condition": "equal",
          "rightcolumn": "LIFNR,SOURCE_ID",
          "dropcolumn": "",
          "onconflict":"PPV_,VD_"
        }
      ]
    },
    {
    "name": "PO_SOURCING",
      "type": "filter",
      "source": "PO_SOURCING",
      "priority": 16,
      "cache": false,
      "conditions": [
        {
          "condition": "COLNAMEFROMFILE",
          "value": ["file","PO.txt"]
        }
      ]
    },
    {
      "name": "PO_SOURCING",
      "type": "query",
      "source": "PO_SOURCING",
      "priority": 17,
      "cache": false,
      "conditions": [
        {
          "condition": "file,selectsqlpo.txt",
          "sql": ""
        }
      ]
    },
    {
    "name": "PPV_output",
      "type": "filter",
      "source": "PPV",
      "priority": 18,
      "cache": false,
      "conditions": [
        {
          "condition": "selectexpr",
          "value": ["file","selectexprfile_ppvreconcile.txt"],
          "datatype": "String"
        }
      ]
    }
  ],
  "outputs": [
    {
      "name": "PPV",
      "location": "/Users/vsingh007c/Downloads/",
      "sources": ["PPV"],
      "format": "parquet",
      "savemode": "overwrite",
      "selectedColumns": []
    }
  ]
}
```
