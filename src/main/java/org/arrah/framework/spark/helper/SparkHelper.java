package org.arrah.framework.spark.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.arrah.framework.jsonparser.DatasourceParser;

public class SparkHelper {

	private static final Logger logger = Logger.getLogger(SparkHelper.class.getName());
	
	public static Dataset<Row> getDataFrame(SparkSession sqlContext, String format, String inputPath, String header) {

		Dataset<Row> dataFrame = null;
		String localheader = "false";
		if (!header.equalsIgnoreCase("true")) {
			localheader = "false";
		} else {
			localheader = "true";
		}

		try {
			if (format.equals("csv")) {
				dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", localheader)
						.option("inferSchema", "true").load(inputPath);
			} else if (format.equals("parquet")) {
				dataFrame = sqlContext.read().format("parquet").load(inputPath);
			} else if (format.equals("tab")) {
				dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", localheader)
						.option("inferSchema", "true").option("delimiter", "\\t").load(inputPath);
			} else {
				dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", localheader)
						.option("inferSchema", "true").option("delimiter", format).load(inputPath);
			}

			return dataFrame;

		} catch (Exception e) {
			logger.severe(("Exception in get Dataframe util - " + e.getMessage()));
		}
		return null;
	}

	// This function for JDBC
	private static Dataset<Row> getDataFrameJDBC(SparkSession sqlContext, DatasourceParser ds) {
	
		try {
			DataFrameReader dataFrameR = sqlContext.read().format("jdbc");
			String jdbcparam = ds.getJdbcParam();
			if (jdbcparam != null && jdbcparam.isEmpty() == false) {
			Map<String,Object> confhash = SparkHelper.toHashmap(jdbcparam.split(","), 2);
				for (String s:confhash.keySet())  { // set  params
					dataFrameR = dataFrameR.option(s, confhash.get(s).toString());
					// System.out.println(s + ":" + confhash.get(s).toString());
				}
			}
			
			return dataFrameR.load();
		} catch (Exception e) {
			logger.severe(("Exception in get Dataframe util - " + e.getMessage()));
		}
		return null;
	}
	
	/***
	private static Dataset<Row> getDataFrame(SparkSession sqlContext, String format, String inputPath) {

		Dataset<Row> dataFrame = null;
		String localheader = "false";
		String header = "true";
		if (!header.equalsIgnoreCase("true")) {
			localheader = "false";
		} else {
			localheader = "true";
		}

		try {
			if (format.equals("csv")) {
				dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", localheader)
						.option("inferSchema", "true").load(inputPath);
			} else if (format.equals("parquet")) {
				dataFrame = sqlContext.read().format("parquet").load(inputPath);
			} else if (format.equals("tab")) {
				dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", localheader)
						.option("inferSchema", "true").option("delimiter", "\\t").load(inputPath);
			} else {
				dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", localheader)
						.option("inferSchema", "true").option("delimiter", format).load(inputPath);
			}

			return dataFrame;

		} catch (Exception e) {
			logger.severe(("Exception in get Dataframe util - " + e.getMessage()));
		}
		return null;
	}
	**/
 
	/**  Depreacated - no more in use  -- Dataset<Row> getDataFrame(SparkSession sqlContext, DatasourceParser ds)
	private static Dataset<Row> getDataFrame(SparkSession sqlContext, String format, String inputPath,
			List<String> selectedColumns) {

		return getSelectedColumns(getDataFrame(sqlContext, format, inputPath), selectedColumns);
	}
	**/
	
	// To pass more infor
	public static Dataset<Row> getDataFrame(SparkSession sqlContext, DatasourceParser ds) {
		if (ds == null)  {
			logger.severe("Could not process DataFrame:");
			return null;
		}
		
		String format = ds.getFormat();
		if (format == null || "".equals(format)) {
			logger.severe("Could not process Format:");
			return null;
		}
		String header = ds.getHeader();
		
		if (format.compareToIgnoreCase("jdbc") == 0) {
			return getSelectedColumns(getDataFrameJDBC(sqlContext, ds), ds.getSelectedColumns());
		} else if (header == null || "".equals(header))// if not jdbc old call with header /without header
			return getSelectedColumns(getDataFrame(sqlContext, format, ds.getLocation(),"true"), ds.getSelectedColumns());
		else
			return getSelectedColumns(getDataFrame(sqlContext, format, ds.getLocation(),header), ds.getSelectedColumns());
	}
	
	public static List<String> requiredCoulmns(List<String> columnList , List<String> selectedColumns) {
		
		//Loop through selectedColumns list
		//If the string is there in aidb dataframe ignore it
		//If string is not there in AIDB then add this to a new list. this list is the list to be deleted.
		
		//from the segment data list remove these columns
		 
		List<String> columnsToBeSelected = new ArrayList<String>();
		 
		for(String columnName : selectedColumns) {
			
			if(columnList.contains(columnName)) {
				columnsToBeSelected.add(columnName);
			}
		}
		
		return columnsToBeSelected;
	}
	
	public static Dataset<Row> getSelectedColumns(Dataset<Row> df, List<String> selectedColumns) {

		// scala.collection.Seq seqCols =
		// scala.collection.JavaConverters.asScalaIteratorConverter(selectedColumns.subList(1,
		// selectedColumns.size()-1).iterator()).asScala().toSeq();
		// scala.collection.Seq<String> seqCols =
		// scala.collection.JavaConverters.asScalaIteratorConverter(selectedColumns.iterator()).asScala().toSeq();

		List<String> dropCols = new ArrayList<>();
		if (!selectedColumns.isEmpty()) {
			for (String s : df.columns()) {
				if (!selectedColumns.contains(s))
					dropCols.add(s);
			}

			// logger.severe("--------BEFORE---------");
			// df.printSchema();

			for (String col : dropCols) {
				df = df.drop(col);
			}
		}

		// logger.severe("--------AFTER---------");
		// df.printSchema();

		return df;

	}

	public static void writeDataframe(Dataset<Row> df, String outputFormat, String outputPath,
			List<String> selectedColumns, String savemode) {

		writeDataFrame(getSelectedColumns(df, selectedColumns), outputFormat, outputPath,savemode);

	}

	public static void writeDataFrame(Dataset<Row> df, String outputFormat, String outputPath, String savemode) {

		String localheader = "true";
		SaveMode sm = SaveMode.Overwrite;
		if ( savemode.equalsIgnoreCase("append") == true)
			sm = SaveMode.Append;

		if (outputFormat.equalsIgnoreCase("csv") || outputFormat.equalsIgnoreCase("parquet")) {

			df.write().mode(sm).option("header", localheader).format(outputFormat).save(outputPath);
		} else if (outputFormat.equalsIgnoreCase("tab")) {
			df.write().mode(sm).format("com.databricks.spark.csv").option("header", localheader)
					.option("delimiter", "	").save(outputPath);
		} else {
			df.write().mode(sm).format("com.databricks.spark.csv").option("header", localheader)
					.option("delimiter", outputFormat).save(outputPath);
		}
	}
	
	public static JavaPairRDD<String,Row> dfToPairRDD(String keyColumn,Dataset<Row> df){
		
		return df.toJavaRDD().keyBy(row -> row.getAs(keyColumn).toString());
		
	}
	
	public static DataType getDatatype(String rdataType) {
		
		DataType dt = DataTypes.StringType;
		
		if(rdataType != null){
		
			if (rdataType.equalsIgnoreCase("string"))
				dt= DataTypes.StringType;
			else if (rdataType.equalsIgnoreCase("double"))
				dt= DataTypes.DoubleType;
			else if (rdataType.equalsIgnoreCase("long"))
				dt= DataTypes.LongType;
			else if (rdataType.equalsIgnoreCase("integer"))
				dt= DataTypes.IntegerType;
			else if (rdataType.equalsIgnoreCase("date"))
				dt= DataTypes.DateType;
			else if (rdataType.equalsIgnoreCase("float"))
				dt= DataTypes.FloatType;
			else if (rdataType.equalsIgnoreCase("decimal"))
				dt= DataTypes.createDecimalType();
			else
				dt= DataTypes.StringType; // default
		}
		return dt;
	}
	
	// HashList
	public static Map<String,Object> toHashmap(String[] attributeList, int tupleSize) {
		HashMap<String,Object> condMap = new HashMap<String,Object>();
		
		for (int i=0; i < attributeList.length; i=i+tupleSize) {
			String tupVal="";
			for (int j=1; j < tupleSize; j++)
				tupVal += attributeList[i+j];
			condMap.put(attributeList[i], tupVal);
		}
		return condMap;
	}
	
	// OrderedList
	public static List<String[]> toOrderedList(String[] attributeList, int tupleSize) {
		List<String[]> condMap = new ArrayList<String[]>();
		for (int i=0; i < attributeList.length; i=i+tupleSize) {
			String[] newtuple = new String[tupleSize];
			for (int j=0; j < tupleSize; j++) 
				newtuple[j] = attributeList[i+j];
			condMap.add(newtuple);
		}
		return condMap;
	}

}
