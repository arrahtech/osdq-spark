package org.arrah.dq.spark.core.dataprocess;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



public class DataNormalizationUtil implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataNormalizationUtil() {

	}
	
	// Use model to normalize zscore
	
	public Dataset<Row> zscore(DataFrameProperty inputBean, final double mean, final double stddev) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String uniqCol = inputBean.getJoinableColName();
		SparkSession sqlContext = inputBean.getSparkSession();

		Dataset<Row> newdf = df.select(labelCol, uniqCol);
		JavaRDD<Row> parseddata = null ;
		
		try {
		
			parseddata = newdf.javaRDD().map(new Function<Row, Row>() {
			/**
			 * Anonymous function
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row r) throws Exception {
				System.out.println("New DF");
				Double regv = r.getDouble(0);
				double newVal = (regv - mean )/stddev;
				System.out.println("Val:"+ newVal);
				// return newVal;
				return RowFactory.create(newVal, r.getDouble(1));
				
			}
		});
		} catch(Exception e) {
			System.out.println("Exception");
		}
		System.out.println("Schema");
		// Generate the schema based on the string of schema
		StructField[] fields = new StructField[2];
		fields[0] = DataTypes.createStructField(labelCol, DataTypes.DoubleType, true);
		fields[1] = DataTypes.createStructField(uniqCol, DataTypes.DoubleType, true);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
		df.join(newdf1, uniqCol);
		return df;
	}

	// Use model to normalize zero score
	
	public Dataset<Row> zeroscore(DataFrameProperty inputBean, final double min, final double max) {

		Dataset<Row> df = inputBean.getDataFrame();
		String labelCol = inputBean.getPrimaryCol();
		String uniqCol = inputBean.getJoinableColName();
		SparkSession sqlContext = inputBean.getSparkSession();

		Dataset<Row> newdf = df.select(labelCol, uniqCol);

		JavaRDD<Row> parseddata = newdf.javaRDD().map(new Function<Row, Row>() {
			/**
			 * Anonymous function
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Row r) throws Exception {
				Double regv = r.getDouble(0);
				double newVal = (regv - min )/(max - min);
				return RowFactory.create(newVal, r.getDouble(1));
				
			}
		});

		// Generate the schema based on the string of schema
		StructField[] fields = new StructField[2];
		fields[0] = DataTypes.createStructField(labelCol, DataTypes.DoubleType, true);
		fields[1] = DataTypes.createStructField(uniqCol, DataTypes.DoubleType, true);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
		Dataset<Row> retdf = df.join(newdf1, uniqCol);
		return retdf;
	}
	
	// Use model to normalize ratio
	
		public Dataset<Row> ratioscore(DataFrameProperty inputBean, final double rvalue) {

			Dataset<Row> df = inputBean.getDataFrame();
			String labelCol = inputBean.getPrimaryCol();
			String uniqCol = inputBean.getJoinableColName();
			SparkSession sqlContext = inputBean.getSparkSession();

			Dataset<Row> newdf = df.select(labelCol, uniqCol);

			JavaRDD<Row> parseddata = newdf.javaRDD().map(new Function<Row, Row>() {
				/**
				 * Anonymous function
				 */
				private static final long serialVersionUID = 1L;

				public Row call(Row r) throws Exception {
					Double regv = r.getDouble(0);
					double newVal = (regv/rvalue);
					return RowFactory.create(newVal, r.getDouble(1));
					
				}
			});

			// Generate the schema based on the string of schema
			StructField[] fields = new StructField[2];
			fields[0] = DataTypes.createStructField(labelCol, DataTypes.DoubleType, true);
			fields[1] = DataTypes.createStructField(uniqCol, DataTypes.DoubleType, true);

			StructType schema = DataTypes.createStructType(fields);

			Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
			Dataset<Row> retdf = df.join(newdf1, uniqCol);
			return retdf;
		}
		
		// Use model to normalize subtraction
		
		public Dataset<Row> substractionscore(DataFrameProperty inputBean, final double rvalue) {

			Dataset<Row> df = inputBean.getDataFrame();
			String labelCol = inputBean.getPrimaryCol();
			String uniqCol = inputBean.getJoinableColName();
			SparkSession sqlContext = inputBean.getSparkSession();

			Dataset<Row> newdf = df.select(labelCol, uniqCol);

			JavaRDD<Row> parseddata = newdf.javaRDD().map(new Function<Row, Row>() {
				/**
				 * Anonymous function
				 */
				private static final long serialVersionUID = 1L;

				public Row call(Row r) throws Exception {
					Double regv = r.getDouble(0);
					double newVal = (regv-rvalue);
					return RowFactory.create(newVal, r.getDouble(1));
					
				}
			});

			// Generate the schema based on the string of schema
			StructField[] fields = new StructField[2];
			fields[0] = DataTypes.createStructField(labelCol, DataTypes.DoubleType, true);
			fields[1] = DataTypes.createStructField(uniqCol, DataTypes.DoubleType, true);

			StructType schema = DataTypes.createStructType(fields);

			Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
			Dataset<Row> retdf = df.join(newdf1, uniqCol);
			return retdf;
		}
						

	
}
