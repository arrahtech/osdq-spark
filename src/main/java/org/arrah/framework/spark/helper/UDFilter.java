package org.arrah.framework.spark.helper;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.FilterFunction;


/*
 * Runner
 * 
 * /opt/mapr/spark/spark-1.6.1/bin/spark-submit -v --master yarn --jars /home/hmakam001c/ACESample/spark-csv_2.10-1.4.0.jar,/home/hmakam001c/ACESample/commons-csv-1.3.jar --class com.comcast.athena.ace.spark.run.ACESegmentTimeRunner /home/hmakam001c/ACE/ACESpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c /home/hmakam001c/ACE/config_seg_time_int.json
 */
public class UDFilter {
	private static final Logger logger = Logger.getLogger(UDFilter.class.getName());

	public UDFilter () { // Default constructor
		
	}
	
	public static List<? extends Object> parseForValues(List<? extends Object> parameter) {
		String s = parameter.get(0).toString();
		
		if(s.compareToIgnoreCase("constant") ==0) {
			parameter.remove(0); // remove first 
			return  parameter;
			
		} else if(s.compareToIgnoreCase("file") ==0) { // take parameter from File
			String fileName = parameter.get(1).toString();
			// Now parse the file and return List of objects
			return parseFile(fileName);
		}
		else {
			// custom build functions are called
			// it maybe in loop like  // userdefined,funcname(),param1,param2,funcname(),param4.......
			// start from right most 
			// 
			String udFunction = parameter.get(1).toString(); // userdefined,funcname(),param1,param2.......
			parameter.remove(0); parameter.remove(0);// remove first and then function name
			return callUDFunction( udFunction,  parameter );
		}
		
	}
	
	private static List<? extends Object> callUDFunction(String udFunction, List<? extends Object> parameter ) {
		List<Object> newval = new ArrayList<Object> ();
		switch (udFunction) {
		case "now":
			Long l =System.currentTimeMillis();
			newval.add(l);
			return newval;
		case "datenow":
			Object s = dateNow(parameter.get(0).toString()); // one value - format
			newval.add(s);
			return newval;
			
		default:
				return parameter;
			
		}
	}
	
	private static Object dateNow(String format) {
		DateFormat df = new SimpleDateFormat(format);
		Date dateobj = new Date();
		return df.format(dateobj);
	}
	
	
	// user defined filter function for single column
    @SuppressWarnings("unused")
	private  class myfilter<T> implements FilterFunction<T> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public boolean call(T value) throws Exception {
            return (Integer)value > 12;
        }
    };
    
    private static List<? extends Object> parseFile(String fileName) {
    	List<String> selectexpr = new ArrayList<String>();
    		try {
    			File keyfile = new File(fileName);
    			Path path = Paths.get(keyfile.getPath());
    			List<String> lines = Files.readAllLines(path,StandardCharsets.ISO_8859_1);
    			String multiline="";
    			for (String line: lines) {
    				line = line.trim();
    				char c = line.charAt(line.length() -1); // last character
    				if (c == ',') {
    					line =line.substring(0, line.length() -1);
    					if ("".equals(multiline) )
    						selectexpr.add(line);
    					else
    						selectexpr.add(multiline+line);
    					multiline="";
    				}
    				else if (c =='\\') {
    					multiline += line.substring(0, line.length() -1)+" ";
    				}
    				
    			}
    			if ("".equals(multiline) == false) {
    				logger.severe("Multiline is not  closed properly");
    			}
    			
    			return selectexpr;
    			
    		} catch (Exception e) {
    			logger.severe(e.getLocalizedMessage());
    			return selectexpr;
    		}
    	
    }
}
