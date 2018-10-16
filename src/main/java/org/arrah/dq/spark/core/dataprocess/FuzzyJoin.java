package org.arrah.dq.spark.core.dataprocess;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class FuzzyJoin implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FuzzyJoin() {

	}

	// This function will take do columns and based on fuzzy logic will do join
	public Dataset<Row> doFuzzReplace(DataFrameProperty inputBean, ArrayList<String> mastervalue, double fuzzyI) {
		Dataset<Row> df = inputBean.getDataFrame();
		if (mastervalue == null || mastervalue.size() == 0)
			return df;
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
				Object strval = r.get(0);
				if (strval == null || "".equals(strval.toString()) )
						return RowFactory.create(strval, r.getDouble(1));
				
				// Iterate through another loop
				for (String a: mastervalue) {
					double dist = CosineDistance.showCosineDistance(a, strval.toString());
					System.out.println(a+":" + strval.toString() + ":"+dist);
					if (dist >= fuzzyI) { // it matches the threshold
						strval  = new String(a);
						break;
					}
				}
				return RowFactory.create(strval, r.get(1).toString());
				
			}
		});

		// Generate the schema based on the string of schema
		StructField[] fields = new StructField[2];
		fields[0] = DataTypes.createStructField(labelCol, DataTypes.StringType, true);
		fields[1] = DataTypes.createStructField(uniqCol, DataTypes.StringType, true);

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> newdf1 = sqlContext.createDataFrame(parseddata, schema);
		newdf1 = newdf1.withColumnRenamed("C13", "UPDATEDCOL");
		newdf1.show(20);
		Dataset<Row> retdf = df.join(newdf1, uniqCol);
		return retdf;
	}

}