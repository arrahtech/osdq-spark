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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.arrah.dq.spark.core.datasampler.cli.Validator;
import org.arrah.dq.spark.core.datasampler.samplers.StratifiedSampler;
import org.arrah.dq.spark.core.datasampler.samplers.RandomSampler;
import org.arrah.dq.spark.core.datasampler.samplers.RootSampler;
import org.arrah.dq.spark.core.datasampler.samplers.StratifiedSamplerExact;
import org.arrah.framework.spark.helper.SparkHelper;

public class ComputeSample {
	private static final Logger logger = Logger.getLogger(ComputeSample.class.getName());

	public static void main(String[] args) {

		Validator cmdValidator = new Validator(args);

		CommandLine cmd = cmdValidator.parse();

		SparkConf conf = new SparkConf().setAppName("DataSampler");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		//JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
		
		//javaSparkContext.setLogLevel("ERROR");
		String inputPath = cmd.getOptionValue("i");
		String outputPath = cmd.getOptionValue("o");
		String inputFormat = cmd.getOptionValue("if");
		String outputFormat = cmd.getOptionValue("of");
		String sampleType = cmd.getOptionValue("t");
		String keyColumn = cmd.getOptionValue("c");
		String keyMappings = cmd.getOptionValue("fm");
		String header = cmd.getOptionValue("hr");
		
		double fraction = (new Double(cmd.getOptionValue("f"))).doubleValue();

		Dataset<Row> df = SparkHelper.getDataFrame(spark, inputFormat, inputPath,header);

		Dataset<Row> fDf = null;

		RootSampler sampler = null;
		
		logger.info("keyColumn - : "+keyColumn);
		logger.info("sampleType - : "+sampleType);

		Map<String,Double> fractionsMap = new HashMap<>();
		
		if(sampleType.equalsIgnoreCase("stratified") || sampleType.equalsIgnoreCase("keylist")){
		
			try {
				Scanner scanner = new Scanner(new FileReader(keyMappings));
				
			
				while (scanner.hasNext()) {
				     String line = scanner.next();
				     fractionsMap.put(line.split(",")[0],new Double(line.split(",")[1]));
				     
				}
				scanner.close();
			} catch (FileNotFoundException e) {
				logger.severe("keymappings file not found");
				System.exit(1);
			}

		}
		
		if (sampleType.equalsIgnoreCase("stratified")) {
			logger.info("in Stratified Sampling");
			sampler = new StratifiedSamplerExact(fractionsMap,fraction,keyColumn);
			
		} else if (sampleType.equalsIgnoreCase("keylist")) {
			logger.info("In Keylist sampling ");
			sampler = new StratifiedSampler(fractionsMap, fraction, keyColumn);

		} else {
			logger.info("In random sampling ");

			sampler = new RandomSampler(fraction);
		}
			fDf = sampler.sample(df);

			String localheader = "false";
			if(!header.equalsIgnoreCase("true")){
				localheader = "false";
			}else{
				localheader="true";
			}
			
			
		if(fDf != null){
			
			if(outputFormat.equalsIgnoreCase("csv") ||outputFormat.equalsIgnoreCase("parquet") ){
			
			fDf.write().mode(SaveMode.Overwrite).option("header", localheader).format(outputFormat).save(outputPath);
		}else if(outputFormat.equalsIgnoreCase("tab")){
			fDf.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", localheader).option("delimiter","	").save(outputPath);
		}else{
			fDf.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", localheader).option("delimiter",outputFormat).save(outputPath);
		}
		}
		
		
	}
}
