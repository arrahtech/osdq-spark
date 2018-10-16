package org.arrah.framework.spark.stgdataframe;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.arrah.framework.jsonparser.ConditionParser;
import org.arrah.framework.spark.helper.SparkHelper;



public class EnrichDF {

	private static final Logger logger = Logger.getLogger(EnrichDF.class.getName());
	
	public static Dataset<Row>  enrichDF(Dataset<Row>  df, List<ConditionParser> conditions) {

		Dataset<Row>  retDF = df;

		for (ConditionParser c : conditions) {
			retDF = addenrichDF(retDF, c);
			}

		return retDF;
	}
	
	private static Dataset<Row>  addenrichDF(Dataset<Row>  df, ConditionParser conditions) {

		Dataset<Row>  retDF = null;
			String condition = conditions.getCondition();
			// there maynot be group by as aggr can be done on compete dataset
			if (condition.equalsIgnoreCase("nullreplace")) {
				
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(",");
				Map<String,Object> condMap = SparkHelper.toHashmap(aggrCA,2);
				
				//Column testC = new Column("testcolumn");
				// Only for testing
				//df = df.withColumn("testcolumn", org.apache.spark.sql.functions.lit(null).cast("long"));
				
				//df.printSchema();
				//df.filter(df.col(aggrCA[0]).isNull()).show();
				retDF = df.na().fill(condMap);
				//retDF.filter(retDF.col(aggrCA[0]).isNull()).show();
				return retDF;
			}
			
			else if (condition.equalsIgnoreCase("dropcolumns")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(",");
				retDF = df;
				for (String s:aggrCA) // Drop Columns
					retDF = retDF.drop(s);
				
				return retDF;
			}
			else if (condition.equalsIgnoreCase("renamecolumns")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(",");
				Map<String,Object> condMap = SparkHelper.toHashmap(aggrCA,2);
				
				retDF = df;
				for (String s:condMap.keySet())  // rename Columns
					retDF = retDF.withColumnRenamed(s, condMap.get(s).toString());

				return retDF;
			} else if (condition.equalsIgnoreCase("addcolumns")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(":");
				// Map may not keep the order so replace with ordered pair
				List<String[]> condMap = SparkHelper.toOrderedList(aggrCA, 2);
				retDF = df;
				for (String[] s:condMap)  { // add Columns
					//System.out.println(s + ":" + condMap.get(s));
					retDF = retDF.withColumn(s[0], org.apache.spark.sql.functions.expr(s[1]));
					//retDF = retDF.withColumn(s, org.apache.spark.sql.functions.expr("lit(1)") );
				}
				return retDF;
			} else {
				logger.warning("This Enrichment condition is not supported:" + condition);
			}

		return df; // no match return the original
	}
}
