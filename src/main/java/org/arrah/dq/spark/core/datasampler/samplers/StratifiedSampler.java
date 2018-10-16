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

public class StratifiedSampler extends RootSampler {

	
	private Map<String,Double> fractionsMap;
	private String keyColumn;
	private int listlen =0;
	
	public StratifiedSampler(Map<String,Double> fractionsMap,double defaultFraction,String keyColumn){
		
		this.keyColumn = keyColumn;
		this.fraction = defaultFraction;
		this.fractionsMap = fractionsMap;

	}
	
	// helper constructor for 
	public StratifiedSampler(List<String> fractionsMapRaw,double defaultFraction,String keyColumn){
		
		this.keyColumn = keyColumn;
		this.fraction = defaultFraction;
		listlen = fractionsMapRaw.size();
		
		this.fractionsMap = createFractionMap(fractionsMapRaw);

	}
	
	private Map<String,Double> createFractionMap(List<String> fractionsMapRaw) {
		Map<String,Double> fractionsMapN = new HashMap<String,Double>();
		if (fractionsMapRaw == null|| fractionsMapRaw.isEmpty() || listlen ==0) // default disproportinate
			return fractionsMapN;
		for (int i=0; i< (listlen-1); i=i+2) {
			try {
				fractionsMapN.put(fractionsMapRaw.get(i),Double.parseDouble(fractionsMapRaw.get(i+1)) );
			} catch(Exception e) {
				System.out.println("Key:" + fractionsMapRaw.get(i));
				System.out.println("Could not parse fraction value:" + e.getLocalizedMessage());
			}
		}
		return fractionsMapN;
				
	}
	@Override
	public Dataset<Row> sample(Dataset<Row> df) {
		
		List<Row> uniqueKeys = df.select(keyColumn).distinct().collectAsList();
		
		 for(Row uniqueKey:uniqueKeys){
			 /**If the specified key is not already associated with a value or 
			  * is associated with null, associates it with the given non-null value. 
			  * Otherwise, replaces the associated value with the results of the given 
			  * remapping function, or removes if the result is null. 
			  * This method may be of use when combining multiple mapped values for a key. 
			  * For example, to either create or append a String msg to a value mapping:
			  */
			 
			 // default value 0
			 fractionsMap.merge(uniqueKey.mkString(),fraction, (V1,V2) -> V1);
			 
		 }
		
		 JavaPairRDD<String,Row> dataPairRDD = SparkHelper.dfToPairRDD(keyColumn, df);
		 
		 //System.out.println("FractionMap:" + fractionsMap);
		 
		 JavaRDD<Row> sampledRDD = dataPairRDD.sampleByKey(false, fractionsMap).values();
		
		 Dataset<Row> sampledDF = df.sqlContext().createDataFrame(sampledRDD, df.schema());
		  
		  return sampledDF;
		  
	}

}
