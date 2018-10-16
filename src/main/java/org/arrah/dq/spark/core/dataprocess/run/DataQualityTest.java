package org.arrah.dq.spark.core.dataprocess.run;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arrah.dq.spark.core.dataprocess.DataFrameProperty;
import org.arrah.dq.spark.core.dataprocess.DataQualityUtil;
import org.arrah.dq.spark.core.dataprocess.FuzzyJoin;


public class DataQualityTest  implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Hashtable<String,String>  mapReplace = new Hashtable<String,String>();
	// private JavaSparkContext javaSparkContext;
	private SparkSession sqlContext;
	private DataQualityUtil dqu;
	private ArrayList<String> progN = new ArrayList<String>();
	
	
	public DataQualityTest () {
		mapReplace.put("vivek_null", "NULL_REPLACEMENT");
		dqu = new DataQualityUtil();
		progN.add("KAREN");progN.add("CNBCE");
	}
	
	public void testRead()  {
		String masterUrl = "local";
		String applicationName = "testApplication";
		// JavaSparkContext javaSparkContext = new JavaSparkContext(masterUrl, applicationName);
		SparkConf conf = new SparkConf();;
		conf = new SparkConf().setAppName(applicationName).setMaster(masterUrl);

		sqlContext = SparkSession.builder().config(conf).getOrCreate();
		String dataFrameFilePath = "/Users/vivek/Documents/my5000.csv";
		//DataFrame dataFrame = sqlContext.parquetFile(dataFrameFilePath);
		Dataset<Row> dataFrame = sqlContext.read().format("com.databricks.spark.csv").load(dataFrameFilePath);
		DataFrameProperty inputBean = new DataFrameProperty();
		
		
		inputBean.setDataFrame(dataFrame);
		inputBean.setPrimaryCol("C13");
		inputBean.setSecondaryCol("d");
		inputBean.setJoinableColName("C18");
		inputBean.setSparkSession(sqlContext);

		String[] colName = dataFrame.columns();
		
		dataFrame.show(20);
		
		Dataset<Row> newdf = new FuzzyJoin().doFuzzReplace(inputBean, progN, 0.8D);
		
		newdf.show(20);
		
		
		
		for (int i = 0; i < colName.length; i++) {
			dataFrame.describe(colName[i]).show();
			System.out.print(" Null Count: " +  dataFrame.where(dataFrame.col(colName[i]).isNull()).count());
			System.out.println(" Unique Count: " + ( dataFrame.groupBy(colName[i]).count() ).select(colName[i]).count());
			dataFrame.groupBy(colName[i]).count().show();
			
			// Replace value
			dataFrame.na().replace(colName[i],mapReplace).show();
			
			System.out.println("ColName :" + colName[i].toString());
		} 
		
		long count = dataFrame.count();
		System.out.println("Total Rows : " + count);
		
		
		LinearRegressionModel model = dqu.doLinearReg(inputBean,20);
		System.out.println("\n Intercept: " + model.intercept());
		System.out.println("Weight :" + model.weights().toString());
		
		
		//DataFrame newdf = dqu.replaceNull(inputBean,model.intercept(),model.weights().toArray()[0]);
		// DataFrame newdf = dqu.replaceNull(inputBean,2,5.0);
		// newdf.show();
		
		inputBean.setDataFrame(newdf);
		//inputBean.setLabelCol("o");
		dqu.replaceValue(inputBean, mapReplace).show();
		
		//String newPath="hdfs://vivek:8020/dataframe/new.csv";
		//DataPreparationUtil dqu2 = new DataPreparationUtil();
		// dqu2.saveCSV(inputBean, SaveMode.Overwrite, newPath);
		// DataFrame df = dqu2.zscore(inputBean, 200000, 300000);
		
		//df.show(30);
		
		
	}

	
	public static void main(String[] args)  {
		Logger rootLogger = Logger.getRootLogger(); 
        rootLogger.setLevel(Level.WARN);
        
        Logger.getLogger("org").setLevel(Level.WARN) ;
        Logger.getLogger("akka").setLevel(Level.WARN) ;
		
		new DataQualityTest().testRead();
	}
}
