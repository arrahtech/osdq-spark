package org.arrah.dq.spark.core.dataprocess;

import java.util.Hashtable;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;

public class DataQualityUtil implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataQualityUtil() {

	}

	// Create and Train Model
	public LinearRegressionModel doLinearReg(DataFrameProperty inputBean, int numIterations) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String regCol = inputBean.getSecondaryCol();

		Dataset<Row> newdf = df.select(labelCol, regCol);

		JavaRDD<LabeledPoint> parseddata = newdf.javaRDD().map(new Function<Row, LabeledPoint>() {
			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			public LabeledPoint call(Row r) throws Exception {
				Object labVObj = r.get(0);
				Object regVarObj = r.get(1);
				if (labVObj != null && regVarObj != null) {
					double[] regv = new double[] { (Double) regVarObj };
					Vector regV = new DenseVector(regv);
					return new LabeledPoint((Double) labVObj, regV);
				} else {
					double[] regv = new double[] { 0.0 };
					Vector regV = new DenseVector(regv);
					return new LabeledPoint(0.0, regV);
				}
			}
		});

		// Building the model
		return LinearRegressionWithSGD.train(parseddata.rdd(), numIterations);

	}

	// Replace Null Value
	public Dataset<Row> replaceNull(DataFrameProperty inputBean, final double intercept, final double weight) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String regCol = inputBean.getSecondaryCol();
		String uniqCol = inputBean.getJoinableColName();
		SparkSession sqlContext = inputBean.getSparkSession();

		Dataset<Row> newdf = df.select(labelCol, regCol, uniqCol);
		
		JavaRDD<Row> parseddata = newdf.toJavaRDD().map(new FunctionMap(intercept,weight));

		// Generate the schema based on the string of schema
		StructField[] fields = new StructField[3];
		fields[0] = DataTypes.createStructField(labelCol, DataTypes.DoubleType, true);
		fields[1] = DataTypes.createStructField(regCol, DataTypes.DoubleType, true);
		fields[2] = DataTypes.createStructField(uniqCol, DataTypes.DoubleType, true);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
		Dataset<Row> retdf = df.join(newdf1, uniqCol);
		return retdf;
	}

	// Replace String Value
	public Dataset<Row> replaceValue(DataFrameProperty inputBean, Hashtable<String, String> newVal) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		return df.na().replace(labelCol, newVal);
	}

	// Replace Double Null Value
	public Dataset<Row> replaceNullValue(DataFrameProperty inputBean, final double val) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String[] cols = new String[] { labelCol };
		Dataset<Row> fill = df.na().fill(val, cols);
		return fill;
	}

	// Replace String Null Value
	public Dataset<Row> replaceNullString(DataFrameProperty inputBean, final String val) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String[] cols = new String[] { labelCol };
		Dataset<Row> fill = df.na().fill(val, cols);
		return fill;
	}
	
	// Will return duplicate column values in sorted order
	// -1 will return all values
	public Dataset<Row> showDuplicateVal(DataFrameProperty inputBean,int limit) {
		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		Dataset<Row> fill = df.select(labelCol);
		if (limit != -1)
			fill = fill.groupBy(labelCol).count().sort().limit(limit);
		else
			fill = fill.groupBy(labelCol).count().sort();
		return fill;
	}
	
	// Will return profile column values in 
	public Hashtable<String,Hashtable<String,Object>> showProfile(DataFrameProperty inputBean, Hashtable<String, Object> value) {

		Hashtable<String,Hashtable<String,Object>> result = new Hashtable<String,Hashtable<String,Object>>();
		Dataset<Row> df = inputBean.getDataFrame();
		String[] colName = df.columns();
		// Loop for each column
		for (int i=0; i < colName.length; i++) {
			Dataset<Row> coldf = df.select(colName[i]);
			Hashtable<String,Object> valueset = new Hashtable<String,Object>();
			try {
				long colC = coldf.count(); // count
				valueset.put("count", colC);
				long uniqueC = coldf.distinct().count(); // unique
				valueset.put("unique", uniqueC);
				long nullC = coldf.na().drop().count(); // null count
				valueset.put("nullcount", nullC - colC);
				
				RelationalGroupedDataset gd = coldf.groupBy(colName[i]);
				Dataset<Row> patterndf = gd.count();
				long patternC = patterndf.where(patterndf.col("count").gt(1)).count();
				valueset.put("pattern", patternC);
				Object mino = coldf.sort().limit(1).toString();
				try {
					Double mind = Double.parseDouble(mino.toString());
					valueset.put("min", mind);
				} catch (Exception e) {
					valueset.put("min", "NA");
				}
				
				Object maxo = coldf.sort(coldf.col(colName[i]).desc()).limit(1).toString();
				try {
					Double maxd = Double.parseDouble(maxo.toString());
					valueset.put("max", maxd);
				} catch (Exception e) {
					valueset.put("max", "NA");
				}
				
				result.put(colName[i], valueset);
			} catch (Exception e) {
				
				continue;
			}
			
		}
		return result;
	}


	// Use model to show dataholes
	public Dataset<Row> showDataHole(DataFrameProperty inputBean) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		SparkSession sqlContext = inputBean.getSparkSession();

		Dataset<Row> newdf = df.select(labelCol);
		newdf.sort(labelCol);

		JavaRDD<Row> parseddata = newdf.javaRDD().map(new Function<Row, Row>() {
			/**
			 * Anonymous function
			 */
			private boolean isInit = false;
			private long pvalue = 0L;
			private static final long serialVersionUID = 1L;

			public Row call(Row r) throws Exception {
				if (r.get(0) == null) {
					return RowFactory.create(-1L); // Null is negative
				} else {
					long cvalue = r.getLong(0);
					if (isInit == false) {
						isInit = true;
						pvalue = cvalue;
					}
					long diff = cvalue - pvalue;
					pvalue = cvalue;
					return RowFactory.create(diff);
				}
			}
		});

		// Generate the schema based on the string of schema
		StructField[] fields = new StructField[1];
		fields[0] = DataTypes.createStructField(labelCol, DataTypes.LongType, true);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
		return newdf1;
	}
	
	// Create and Train Model
	public LinearRegressionModel doMultiLinearReg(DataFrameProperty inputBean, int numIterations) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String [] inputCols = inputBean.getInputCols();

		Dataset<Row> newdf = df.select(labelCol, inputCols);

		JavaRDD<LabeledPoint> parseddata = newdf.javaRDD().map(new Function<Row, LabeledPoint>() {
			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			public LabeledPoint call(Row r) throws Exception {
				Object labVObj = r.get(0);
				int colC = r.size();
				double[] regv = new double[colC -1]; // -1 for first index
				
				for (int i =1; i < colC; i++) {
					Object regVarObj = r.get(i);
					if (regVarObj != null)
						regv[i-1] = (Double)regVarObj;
					else
						regv[i-1] = 0.0D; // Null replaced with 0.0
				}
				
				Vector regV = new DenseVector(regv);
					
				if (labVObj != null ) {
					return new LabeledPoint((Double) labVObj, regV);
				} else {
					return new LabeledPoint(0.0D, regV);
				}

			}
		});

		// Building the model
		return LinearRegressionWithSGD.train(parseddata.rdd(), numIterations);

	}

	// Drop dataframe with null values
	// how can be "any" or "all"
	public Dataset<Row> dropNullCols(DataFrameProperty inputBean,String how) {

		Dataset<Row> df = inputBean.getDataFrame();
		String [] inputCols = inputBean.getInputCols();
		return df.na().drop(how,inputCols);
	}

	// Drop dataframe with duplicate  values
	// It can be all cols or selected cols
	public Dataset<Row> dropDupRows(DataFrameProperty inputBean,boolean all) {

		Dataset<Row> df = inputBean.getDataFrame();
		String [] inputCols = inputBean.getInputCols();
		if (all == true)
			return df.dropDuplicates();
		else
			return df.dropDuplicates(inputCols);
	}
	
	// Replace null with Random values 
	public Dataset<Row> replaceRandom(DataFrameProperty inputBean) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		
		double min = df.groupBy(labelCol).min(labelCol).first().getDouble(0);
		double max = df.groupBy(labelCol).max(labelCol).first().getDouble(0);
		Random rd = new Random();
		double rdGen = rd.nextDouble();
		double finalrand = min + ((max-min) * ( (1-rdGen) / 1 )) ;
		
		return df.na().fill(finalrand, new String[] {labelCol});

	}
	
	// Use model to replace values
	
	public Dataset<Row> replaceValue(DataFrameProperty inputBean, int dtypecol1, int dtypecol2, final Hashtable<?,?> hashtable) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String uniqCol = inputBean.getJoinableColName();
		SparkSession sqlContext = inputBean.getSparkSession();
		String regCol = inputBean.getSecondaryCol();

		Dataset<Row> newdf = df.select(regCol,labelCol,uniqCol);

		JavaRDD<Row> parseddata = newdf.javaRDD().map(new Function<Row, Row>() {
			/**
			 * Anonymous function
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row r) throws Exception {
				Object regv = r.get(0);
				Object labv = hashtable.get(regv);
				if (labv == null)
					return RowFactory.create(regv, r.get(1),r.getDouble(1));
				else
					return RowFactory.create(regv, labv,r.getDouble(1));
				
			}
		});

		// Generate the schema based on the string of schema
		StructField[] fields = new StructField[3];
		if ( dtypecol1 == 0 ) // 0 for number
			fields[0] = DataTypes.createStructField(regCol, DataTypes.DoubleType, true);
		else
			fields[0] = DataTypes.createStructField(regCol, DataTypes.StringType, true);
		
		if ( dtypecol2 == 0 ) // 0 for number
			fields[1] = DataTypes.createStructField(regCol, DataTypes.DoubleType, true);
		else
			fields[1] = DataTypes.createStructField(regCol, DataTypes.StringType, true);
		
		fields[2] = DataTypes.createStructField(uniqCol, DataTypes.DoubleType, true);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
		Dataset<Row> retdf = df.join(newdf1, uniqCol);
		return retdf;
	}
	
	public static void main(String[] args)  {
		String masterUrl = "local";
		String applicationName = "testApplication";
		// JavaSparkContext javaSparkContext = new JavaSparkContext(masterUrl, applicationName);
		SparkConf conf = new SparkConf();;
		conf = new SparkConf().setAppName(applicationName).setMaster(masterUrl);

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		String dataFrameFilePath = "hdfs://vivek:8020/dataframe/storage/avroParquet/data.parquet";
		Dataset<Row> dataFrame = spark.read().parquet(dataFrameFilePath);
		DataFrameProperty inputBean = new DataFrameProperty();
		
		DataQualityUtil dqu;
		dqu = new DataQualityUtil();
		
		inputBean.setDataFrame(dataFrame);
		inputBean.setPrimaryCol("g");
		inputBean.setSecondaryCol("d");
		inputBean.setJoinableColName("p");
		inputBean.setSparkSession(spark);

		String[] colName = dataFrame.columns();
		Dataset<Row> newdf = dqu.replaceNull(inputBean,2,5.0);
		newdf.show();
		System.out.println(colName);
	}

}
