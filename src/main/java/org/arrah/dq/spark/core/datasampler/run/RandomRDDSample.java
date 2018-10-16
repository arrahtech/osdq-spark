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
package org.arrah.dq.spark.core.datasampler.run;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RandomRDDSample {



	public static void main(String[] args) {
	

		SparkConf conf = new SparkConf().setAppName("DataRDDRandomSampler");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		
		JavaRDD<String> distFile = javaSparkContext.textFile(args[1]);
		
		distFile.sample(false, new Double(args[0]).doubleValue()).saveAsTextFile(args[2]);
		
		javaSparkContext.close();
		return;
		
	}
	
}
