# OSDQ Spark (beta)

__Release Date: 14-Jan-2019__

__Version: 0.0.1__

__Contact us SUPPORT@ARRAHTECH.COM__

OSDQ is suite of modules around data processing, data quality and data science. These modules are built on top of industry leading big data framework Apache Spark. 
Using these module one can simplify their big data tasks like data processing, data quality, profiling and using machine learning like classification.

Logic can be written in JSON which can be given as input.

It takes JSON file as input ( Look wiki pages for JSON format). A very simple example:
  
```json  
{
"name": "MATERIAL",
  "type": "filter",
  "source": "MATERIAL",
  "priority": 1,
  "cache": false,
  "conditions": [
    {
      "condition": "COLNAMEFROMFILE",
      "value": ["file","Material.txt"],
      "datatype": "String"
    },
    {
      "condition": "IN",
      "column":"sector_flag",
      "value": ["constant","CONS","PHRM"],
      "datatype": "String"
    }

  ]
},
{
  "name": "PPV_RECONCILE",
  "type": "join",
  "source": "PPV_RECONCILE,PPA_CONFIGURATION_TABLE",
  "priority": 6,
  "cache": false,
  "conditions": [
    {
      "joinType": "left_outer",
      "leftcolumn": "IM_sourcing_type",
      "condition": "equal",
      "rightcolumn": "value",
      "dropcolumn": "P_,conf_"
    }
  ]
},
{
  "name": "PPV_RECONCILE",
  "type": "enrichment",
  "source": "PPV_RECONCILE",
  "priority": 7,
  "cache": false,
  "conditions": [
    {
      "condition": "renamecolumns",
      "aggrcondition": "description,SOURCING_TYPE,p_jnj_rm_code,jnj_rm_code"
    },
    {
      "condition": "replacecolumns",
      "aggrcondition": "IR_PRICE1:cast(IR_PRICE1 as decimal(30,10))"
    }
  ]
},
{
  "name": "Group_PPV_RECONCILE",
  "type": "aggregate",
  "source": "PPV_RECONCILE",
  "priority": 8,
  "cache": false,
  "conditions": [
    {
      "condition": "groupby",
      "value": [ "P_SOURCE_ID","P_SOURCE_REGION","POSTING_YEAR","POSTING_MONTH","POSTING_YEAR_MONTH","INVOICE_DATE","PLANT_NAME","P_ENV","INVOICE_NUMBER","PURCHASE_DOC_NO","PURCHASE_DOC_LINE_NO","MATERIAL_CODE","MATERIAL_DESC","VENDOR_NAME","P_CATEGORY","SUBCATEGORY","PURCHASING_UOM","STOCKING_UOM","CONVERSION_FACTOR","PURCHASE_CURRENCY","INVOICE_CURRENCY","FCUR","VENDOR_NO","INVOICE_LINENUMBER","POSTING_DATE_DOC","RECEIPT_DATE","MATERIAL_CODE_PRED","MATERIAL_DESC_PRED","SECONDARY_VENDOR_NO","SECONDARY_VENDOR_NAME","VENDOR_COUNTRY","VENDOR_CITY","SECONDARY_VENDOR_COUNTRY","SECONDARY_VENDOR_CITY","MOQ","IOQ","INCOTERM_CODE","INCOTERM_DESC","INCOTERM_LOCATION_COUNTRY","INCOTERM_LOCATION_CITY","PAYMENT_TERMS","PAYMENT_TERMSDESC","PAYMENT_DAYS","VMI_FLAG","LEADTIME","RECEIPT_CURRENCY","PLANT_CURRENCY","PRICE_VALIDITY_START_DATE","PRICE_VALIDITY_END_DATE","SSETRANSACTION","DOCUMENT_CATEGORY","DOCUMENT_CATEGORY_DESC","MATERIAL_TYPE","RECEIPT_DONE","SAP_PLANTCODE","TRANSACTION_TYPE","GR_QT_PO_IND","SAP_MATNR","TRANSACTION_REFNO","EXT_PO_REF_NO","FLAG","P_EMS_MATERIAL_NUMBER","P_SOURCE","AMSIGN","SAP_VENDORCODE","BPVOLUME","TO_DATE","FROM_DATE","SOURCING_TYPE","PO_QUANTITY","PO_type","IR_CURRENCY1","IR_PRICE1","jnj_rm_code","TRADE_NAME","SPEC"],
      "aggrcondition": "sum,INVOICECOUNT,min,ER_ACTUALRATE,min,ER_WAHR,min,ER_BP,min,POUNITPRICE_FCUR_ACTUALRATE,min,POUNITPRICE_FCUR_WAHR,min,POUNITPRICE_FCUR_BP,max,TOTALPO_AMT_DOC_CURRENCY,max,TOTALPO_AMT_FCUR_ACTUALRATE,max,TOTALPO_AMT_FCUR_WAHR,max,TOTALPO_AMT_FCUR_BP,min,INVOICE_PRICE_DOC_CURRENCY,min,INVOICE_PRICE_FCUR_ACTUALRATE,min,INVOICE_PRICE_FCUR_WAHR,min,INVOICE_PRICE_FCUR_BP,sum,INVOICE_SPEND_DOC_CURRENCY,sum,INVOICE_SPEND_FCUR_ACTUALRATE,sum,INVOICE_SPEND_FCUR_WAHR,sum,INVOICE_SPEND_FCUR_BP,min,standard_cost_doc_curr_actual,min,standard_cost_doc_curr_wahr,min,standard_cost_doc_curr_bp,min,STANDARD_COSTPERUNIT_LCUR,sum,TOTAL_AMOUNT_AT_STANDARD_COST_LCUR,sum,TOTAL_AMOUNT_AT_STANDARD_COST_DOC_CUR_ACTUALRATE,sum,TOTAL_AMOUNT_AT_STANDARD_COST_DOC_CUR_WAHR,sum,TOTAL_AMOUNT_AT_STANDARD_COST_DOC_CUR_BP,sum,PPV1_AMOUNT_DOC_CURRENCY,sum,PPV1_AMOUNT_FCUR_ACTUALRATE,sum,PPV1_AMOUNT_FCUR_WAHR,sum,PPV1_AMOUNT_FCUR_BP,sum,PPV2_AMOUNT_DOC_CURRENCY,sum,PPV2_AMOUNT_FCUR_ACTUALRATE,sum,PPV2_AMOUNT_FCUR_WAHR,sum,PPV2_AMOUNT_FCUR_BP,sum,TOTALPPV_DOC_CURRENCY,sum,TOTALPPV_FCUR_ACTUALRATE,sum,TOTALPPV_FCUR_WAHR,sum,TOTALPPV_FCUR_BP,sum,EXPECTEDPPV_DOC_CURRENCY,sum,EXPECTEDPPV_FCUR_ACTUALRATE,sum,EXPECTEDPPV_FCUR_WAHR,sum,EXPECTEDPPV_FCUR_BP,min,RECEIPT_PRICE,min,RECEIPT_PRICE_FCUR_ACTUALRATE,min,RECEIPT_PRICE_FCUR_WAHR,min,RECEIPT_PRICE_FCUR_BP,sum,RECEIPT_SPEND_DOC_CURRENCY,sum,RECEIPT_SPEND_FCUR_ACTUALRATE,sum,RECEIPT_SPEND_FCUR_WAHR,sum,RECEIPT_SPEND_FCUR_BP,min,ACTUAL_EXCHANGE_RATE,min,WEIGHTED_AVERAGE_HEDGE_RATE_WAHR,min,PPVMISMATCH,min,VARIANCE,min,STANDARD_COSTPERUNIT_BASE_CURRENCY,min,PPV2PERCENTAGE,min,SSE_CURRENCY,min,GSS_STATUS,min,INVOICE_EXCHANGE_RATE"
    }
  ]
}
```

__How To Run it__

### From the source

> Get hold of source and run below command. Please note maven should be present in your system to build the application.

> mvn package - This will create a build named "osdq-spark_0.0.1.zip" under target. Unzip the same, go inside the unzipped folder and run below command as given. 

> java -cp .\lib\\*;osdq-spark-0.0.1.jar org.arrah.framework.spark.run.TransformRunner -c .\example\samplerun.json

__You can replace `SampleData` datasource with below to exploare JDBC database:__

```json
    {
      "name": "SampleDataJDBC",
      "format": "jdbc",
      "jdbcparam":"url,jdbc:mysql://localhost:3306/mysql,driver,com.mysql.jdbc.Driver,user,root,password,root,dbtable,(select * from help_category) AS T,partitionColumn,parent_category_id,lowerBound,0,upperBound,10000,numPartitions,10",
      "selectedColumns": []
    }
 ```

>> _For those on windows, you need to have hadoop distribtion unzipped on local drive and HADOOP_HOME set. Also copy [winutils.exe](https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe) from here into HADOOP_HOME\bin_


We can also give a complete spark-submit script like

> "spark-submit --class "org.arrah.framework.spark.run.TransformRunner" --master yarn --deploy-mode cluster --files "Example.json" TransformRunner.jar -c "Example.json"



Please refer to our [Wiki](https://github.com/arrahtech/osdq-spark/wiki) page for details.


__USE CASES__

1.  Convert Legacy SQL to Spark processing
2.  Write new data processing pipeline
3.  Add Data Quality as intermediate steps
4.  Desing Data Science Models  using JSON

