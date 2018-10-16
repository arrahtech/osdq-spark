package org.arrah.framework.spark.helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.arrah.framework.jsonparser.ConditionParser;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

/*
 * Runner
 * 
 * /opt/mapr/spark/spark-1.6.1/bin/spark-submit -v --master yarn --jars /home/hmakam001c/ACESample/spark-csv_2.10-1.4.0.jar,/home/hmakam001c/ACESample/commons-csv-1.3.jar --class com.comcast.athena.ace.spark.run.ACESegmentTimeRunner /home/hmakam001c/ACE/ACESpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c /home/hmakam001c/ACE/config_seg_time_int.json
 */
public class UDFFile {



	public final static UDF1<WrappedArray<String>,String> hourPartArrayAvg =  new UDF1<WrappedArray<String>,String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(WrappedArray<String> t1) throws Exception {
			
			String[] strArr = new String[96];
			t1.copyToArray(strArr);
			
			StringBuffer sb = new StringBuffer("[");
					
			for(int i=0 ; i<96; i++){
				if(strArr[i] != null){

					String numerator = strArr[i].split(":")[0];
					String denominator = strArr[i].split(":")[1];
					
					int value= Math.round(new Long(numerator).longValue()/new Long(denominator).longValue());
					
					sb.append(value).append(i < 95 ? ",":"");
				
				}else{
				sb.append(0).append(i < 95 ? ",":"");
				}
			}
			
			return sb.append("]").toString();
		}
		
	};
	
	public final static UDF2<Object,Object,String> concatUDF =  new UDF2<Object,Object,String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Object a,Object b) throws Exception {
			if (a == null && b != null) return b.toString();
			if (a != null && b == null) return a.toString();
			if (a == null && b == null ) return "";
			return a.toString()+b.toString();
		}
		
	};
	public final static UDF1<Object,String> toLowerUDF =  new UDF1<Object,String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Object a) throws Exception {
			if (a == null ) return "";
			return a.toString().toLowerCase();
		}
		
	};
	public final static UDF1<Object,String> toUpperUDF =  new UDF1<Object,String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Object a) throws Exception {
			if (a == null ) return "";
			return a.toString().toUpperCase();
		}
		
	};
	
	public final static UDF1<Object,Long> toLongUDF =  new UDF1<Object,Long>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Long call(Object a) throws Exception {
			try {
				return  Long.parseLong(a.toString());
			}catch (Exception e) {
				return 0L;
			}
		}
		
	};
	
	public final static UDF1<Object,Double> toDoubleUDF =  new UDF1<Object,Double>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Double call(Object a) throws Exception {
			try {
				return Double.parseDouble(a.toString());
			}catch (Exception e) {
				return 0.00D;
			}
		}
		
	};
	
	public final static UDF1<Object,String> toStringUDF =  new UDF1<Object,String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Object a) throws Exception {
			if (a == null ) return "";
			 return a.toString();
		}
		
	};
	
	
	public final static Dataset<Row> udfMappedDF(Dataset<Row> df, ConditionParser conditions) {
		String udfname = conditions.getFuncName();
		String rdataType = conditions.getDatatype();
		List<String >colgrp = conditions.getValue();
		List<Column> grpcol = new ArrayList<Column> ();
		
		Dataset<Row> retDF = null;
		DataType dt = null;
		dt = SparkHelper.getDatatype(rdataType);
		
		if(udfname.equalsIgnoreCase("concat")) {
			df.sqlContext().udf().register(udfname,UDFFile.concatUDF,dt);
		
			for( String s: colgrp) // it is of format col1 , col2
				grpcol.add( new Column(s) );
			
			// Now call the UDF
			retDF = df.withColumn(conditions.getColumn(), org.apache.spark.sql.functions.callUDF(udfname,
					JavaConversions.asScalaBuffer(grpcol).seq()));
			return retDF;
		} else if (udfname.equalsIgnoreCase("toLower")){
			
			df.sqlContext().udf().register(udfname,UDFFile.toLowerUDF,dt);
			
			for( String s: colgrp) // it is of format col1
				grpcol.add( new Column(s) );
			
			// Now call the UDF
			retDF = df.withColumn(conditions.getColumn(), org.apache.spark.sql.functions.callUDF(udfname,
					JavaConversions.asScalaBuffer(grpcol).seq()));
			return retDF;
		}  else if (udfname.equalsIgnoreCase("toLong")){
			
			df.sqlContext().udf().register(udfname,UDFFile.toLongUDF,dt);
			
			for( String s: colgrp) // it is of format col1
				grpcol.add( new Column(s) );
			
			// Now call the UDF
			retDF = df.withColumn(conditions.getColumn(), org.apache.spark.sql.functions.callUDF(udfname,
					JavaConversions.asScalaBuffer(grpcol).seq()));
			return retDF;
		} 
		
		
		return df;
		
	}
}
