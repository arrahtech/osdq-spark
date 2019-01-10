package org.arrah.framework.spark.run;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.arrah.framework.inputvalidators.TransformRunnerValidator;
import org.arrah.framework.jsonparser.DatasourceParser;
import org.arrah.framework.jsonparser.OutputParser;
import org.arrah.framework.jsonparser.RootConfigParser;
import org.arrah.framework.jsonparser.SparkConfigParser;
import org.arrah.framework.jsonparser.TransformationParser;
import org.arrah.framework.spark.helper.SparkHelper;
import org.arrah.framework.spark.stgdataframe.DatasourceDF;
import org.arrah.framework.spark.stgdataframe.TransformWrapper;

import com.fasterxml.jackson.databind.ObjectMapper;


public class TransformRunner implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(TransformRunner.class.getName());

	public static void main(String[] args) {
		
		logger.info("\n ######################## \n # Welcome to TransformRunner # \n ########################");

		// Get command line and lcoation of JSON file
		TransformRunnerValidator aceVal = new TransformRunnerValidator(args);
		CommandLine cmd = aceVal.parse();
		String configLoc = cmd.getOptionValue("c");
		
		RootConfigParser jsonConfig = null;
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			jsonConfig = mapper.readValue(new File(configLoc), RootConfigParser.class);
		} catch (IOException e) {
			System.err.println(e.getLocalizedMessage());
			logger.severe("Failed to open config json --"+ configLoc);
			return;
		}
		if (jsonConfig == null) {
			System.err.println("No value in json config file");
			logger.severe("No value in json config file --"+ configLoc);
			return;
		}

		 boolean isVerbose = false;
		// Able to read and parse json file
		// Config spark
		SparkConf conf = new SparkConf();
		try {
			SparkConfigParser scp =jsonConfig.getSparkConfig();
			String param=null;
			
			param=scp.getAppName();
			if (param != null && "".equals(param) ==false)
				conf = conf.setAppName(param);
			
			param=scp.getMaster();
			if (param != null && "".equals(param) ==false)
				conf = conf.setMaster(param);
			
			param=scp.getVerbose();
			if (param == null && "".equals(param))
				isVerbose = false;
			else if (param.compareToIgnoreCase("yes") == 0 || param.compareToIgnoreCase("y") == 0 || param.compareToIgnoreCase("true") == 0)
				isVerbose = true;
			else 
				isVerbose = false;
				
			
			param=scp.getNoOfExecutor();
			if (param != null && "".equals(param) ==false)
				conf = conf.set("spark.executor.instances", param);
			
			param=scp.getNoOfCore();
			if (param != null && "".equals(param) ==false)
				conf = conf.set("spark.executor.cores", param);
			
			param=scp.getExecutorMem();
			if (param != null && "".equals(param) ==false)
				conf = conf.set("spark.executor.memory", param);
			
			param = scp.getConfparam();
			if (param != null && param.isEmpty() == false) {
				Map<String,Object> confhash = SparkHelper.toHashmap(param.split(","), 2);
				for (String s:confhash.keySet())  { // set  params
					conf = conf.set(s, confhash.get(s).toString());
					// System.out.println(s + ":" + confhash.get(s).toString());
				}
			}
			
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage());
			logger.warning("Failed to read spark config json - setting default");
		}
		
		if (conf == null)
			conf = new SparkConf().setAppName("TranformRunner").setMaster("local[4]")
					.set("spark.executor.instances", "4")
			      .set("spark.executor.cores", "2")
			      .set("spark.executor.memory","2g");
		// 2 cores on each workers 4 executor 2g memory 4 threads

		conf.set("spark.sql.parquet.binaryAsString","true"); // Use as string -- should come from config file
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		//JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
		

		spark.sparkContext().setLogLevel("WARN");

		
		HashMap<String, Dataset<Row>> dataFramesMap = new HashMap<>();

		if (jsonConfig != null) {

			// Create Datasources
			try {
				List<DatasourceParser> datasources = jsonConfig.getDatasources();
	
				for (DatasourceParser ds : datasources) {
					if (isVerbose == true) 
						System.out.println("loading Dataset:"+ds.getName());
					dataFramesMap.put(ds.getName(), new DatasourceDF(ds).getDataFrame(spark));
				}
			} catch (Exception e) {
				System.err.println("No input data source");
				logger.severe("No input data source for processing");
				return;
			}
			
			// Create Transformations
			List<TransformationParser> transforms = jsonConfig.getTransformations();

			if (transforms != null) {
				int noOfTransforms = transforms.size();
				if ( noOfTransforms >= 1) {
					// At this point it can not handle same priority
					HashMap<Integer, TransformationParser> transformsMap = new HashMap<>();
					for (TransformationParser t : transforms) {
						Integer prp = t.getPriority();
						if (prp == null || prp < 1)
							transformsMap.put(noOfTransforms--, t);
						else
							transformsMap.put(prp, t);
					}
					
					noOfTransforms = transforms.size(); // start again after reducing for default values
					for (int i = 1; i <= noOfTransforms; i++) {
						TransformationParser t = transformsMap.get(i);
						try {
							if (isVerbose == true) 
								System.out.println("Applying transformation to :" + t.getName() + " at Priority:" + i);
							dataFramesMap.put(t.getName(), TransformWrapper.applyTransform(t, dataFramesMap));
						} catch (Exception e) {
							System.err.println("Transforamtion exception" + e.getLocalizedMessage());
							logger.severe("Not able to apply Transform for dataset:" + t.getName());
							System.out.println("Columns:" + Arrays.asList(dataFramesMap.get(t.getSource()).columns()) );
							continue;
						}
					}
		
					if (isVerbose == true) {
						for (String key: dataFramesMap.keySet()) {
							System.out.println(key);
							dataFramesMap.get(key).show();
							//dataFramesMap.get(key).explain();;
							
						}
					}
				}
			} // if null then move to Output directly
			
			List<OutputParser> outputs = jsonConfig.getOutputs();
			for (OutputParser o : outputs) {

				String outputPath = o.getLocation();
				String name = o.getName();
				String saveMode = o.getSavemode();
				String outputFormat = o.getFormat();
				List<String> dfstosaved = o.getSources();

				for (String dfstr:dfstosaved) {
					Dataset<Row> df = dataFramesMap.get(dfstr);
					if (df != null) 
						SparkHelper.writeDataframe(df, outputFormat, outputPath+"/"+name+"/"+dfstr, o.getSelectedColumns(),saveMode);
					} // end of df loop

			} // end of output

		} // end of processing

	} // end of main

}
