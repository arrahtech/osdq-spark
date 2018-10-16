/*******************************************************************************
 * Copyright (c) 2016 
 *
 *   Any part of code or file can be changed,    
 *   redistributed, modified with the copyright  
 *   information intact .                       
 *  
 * Contributors:
 *         Hareesh Makam
 *******************************************************************************/
package org.arrah.dq.spark.core.datasampler.samplers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.arrah.framework.spark.helper.SparkHelper;

public class StratifiedSamplerExact extends RootSampler {

	private Map<String, Double> fractionsMap = new HashMap<String, Double>();
	private List<Row> uniqueKeys;
	private String keyColumn;
	
	public StratifiedSamplerExact(Map<String, Double> fractionsMap,double defaultFraction,String keyColumn){
		
		this.keyColumn = keyColumn;
		this.fraction = defaultFraction;
		this.fractionsMap = fractionsMap;
		
	}

	public StratifiedSamplerExact(List<Row> uniqueKeys,Map<String, Double> fractionsMap,double defaultFraction,String keyColumn){
		
		this.keyColumn = keyColumn;
		this.fraction = defaultFraction;
		this.fractionsMap = fractionsMap;
		this.uniqueKeys = uniqueKeys;
	}
	
	@Override
	public Dataset<Row> sample(Dataset<Row> df) {
		
		if(uniqueKeys == null){
			uniqueKeys = df.select(keyColumn).distinct().collectAsList();
		}
		 for(Row uniqueKey:uniqueKeys){
			 
			 fractionsMap.merge(uniqueKey.mkString(),fraction, (V1,V2) -> V1);
			 
		 }
		
		 JavaPairRDD<String, Row> dataPairRDD = SparkHelper.dfToPairRDD(keyColumn, df);
		 
		  JavaRDD<Row> sampledRDD = dataPairRDD.sampleByKeyExact(false, fractionsMap).values();
		
		  Dataset<Row> sampledDF = df.sqlContext().createDataFrame(sampledRDD, df.schema());
		  
		  return sampledDF;
		  
	}

	
	
}
