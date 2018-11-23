package org.arrah.framework.spark.stgdataframe;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arrah.framework.jsonparser.DatasourceParser;
import org.arrah.framework.spark.helper.SparkHelper;

public class DatasourceDF {
	
	DatasourceParser ds;
	
	public DatasourceDF(DatasourceParser ds){
		
		this.ds=ds;
		
	}
	
	public Dataset<Row>  getDataFrame(SparkSession sqlContext){
		
		Dataset<Row>  df = SparkHelper.getDataFrame(sqlContext, ds);
		// Dataset<Row>  df = SparkHelper.getDataFrame(sqlContext, ds.getFormat(), ds.getLocation(),ds.getSelectedColumns());	
	return df;	
		
	}
	

}
