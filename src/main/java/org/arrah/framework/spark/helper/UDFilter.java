package org.arrah.framework.spark.helper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/*
 * Runner
 * 
 * /opt/mapr/spark/spark-1.6.1/bin/spark-submit -v --master yarn --jars /home/hmakam001c/ACESample/spark-csv_2.10-1.4.0.jar,/home/hmakam001c/ACESample/commons-csv-1.3.jar --class com.comcast.athena.ace.spark.run.ACESegmentTimeRunner /home/hmakam001c/ACE/ACESpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c /home/hmakam001c/ACE/config_seg_time_int.json
 */
public class UDFilter {

	public UDFilter () { // Default constructor
		
	}
	
	public static List<? extends Object> parseForValues(List<? extends Object> parameter) {
		String s = parameter.get(0).toString();
		
		if(s.compareToIgnoreCase("constant") ==0) {
			parameter.remove(0); // remove first 
			return  parameter;
			
		} else {
			// custom build functions are called
			// it maybe in loop like  // userdefined,funcname(),param1,param2,funcname(),param4.......
			// start from right most 
			// 
			String udFunction = parameter.get(1).toString(); // userdefined,funcname(),param1,param2.......
			parameter.remove(0); parameter.remove(0);// remove first 
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
}
