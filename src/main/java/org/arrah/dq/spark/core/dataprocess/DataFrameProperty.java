package org.arrah.dq.spark.core.dataprocess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// This is bean file for get set property which will be pass to other classes

public class DataFrameProperty  implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Input DataFrame
	 */
	private Dataset<Row> df;
	private String primaryColName;  // column on wich operation will be performed
	private String secondaryColName; // secondary column for supporting analysis like regression
	private String joinableColName; // the column on which join operation will be performed. presently it has be double type
	private SparkSession sqlContext;
	private String[] inputCols;
	
	public DataFrameProperty () {
		
	}

	public Dataset<Row> getDataFrame() {
		return df;
	}


	public void setDataFrame(Dataset<Row> df) {
		this.df = df;
	}


	public String getPrimaryCol() {
		return primaryColName;
	}


	public void setPrimaryCol(String labelCol) {
		this.primaryColName = labelCol;
	}


	public String getSecondaryCol() {
		return secondaryColName;
	}


	public void setSecondaryCol(String regCol) {
		this.secondaryColName = regCol;
	}


	public String getJoinableColName() {
		return joinableColName;
	}


	public void setJoinableColName(String uniqColName) {
		this.joinableColName = uniqColName;
	}


	public SparkSession getSparkSession() {
		return sqlContext;
	}


	public void setSparkSession(SparkSession sqlContext) {
		this.sqlContext = sqlContext;
	}

	public String[] getInputCols() {
		return inputCols;
	}

	public void setInputCols(String[] inputCols) {
		this.inputCols = inputCols;
	}
	
	
}
