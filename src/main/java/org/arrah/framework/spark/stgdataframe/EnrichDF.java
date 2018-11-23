package org.arrah.framework.spark.stgdataframe;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.spark.sql.Column;
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
			} else if (condition.equalsIgnoreCase("renameallcolumns")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] colNames = df.columns();
				retDF = df;
				
				for (String colName:colNames)
					retDF = retDF.withColumnRenamed(colName, aggrCond+colName);
				
				return retDF;
			} else if (condition.equalsIgnoreCase("reordercolumns")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(",");
				if (aggrCA == null || aggrCA.length < 1) return df;
				
				String [] withoutF = new String[aggrCA.length -1];
				for (int i=1; i < aggrCA.length; i++)
					withoutF[i-1] = aggrCA[i];
				retDF = df.select(aggrCA[0],withoutF);

				return retDF;
			}
			else if (condition.equalsIgnoreCase("addcolumns")) {
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
			} else if (condition.equalsIgnoreCase("replacecolumns")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(":");
				// Map may not keep the order so replace with ordered pair
				List<String[]> condMap = SparkHelper.toOrderedList(aggrCA, 2);
				retDF = df;
				for (String[] s:condMap)  { // replace Columns
					String tempCol = "tempcoldonotreplicatearrah"; // should not be replacted
					retDF = retDF.withColumn(tempCol, org.apache.spark.sql.functions.expr(s[1]));
					retDF = retDF.drop(s[0]);
					retDF = retDF.withColumnRenamed(tempCol, s[0]);
				}
				return retDF;
			} else if (condition.equalsIgnoreCase("conditionalcolumn")) {
				// Now get aggregate condition Map
				String aggrCond = conditions.getAggrcondition();
				String[] aggrCA = aggrCond.split(":");
				
				retDF = df;
				retDF = parseCond(retDF, aggrCA[1]);
				retDF = retDF.withColumnRenamed("TEMPCOL",aggrCA[0]);
			
				return retDF;
			}
			else {
				logger.warning("This Enrichment condition is not supported:" + condition);
			}

		return df; // no match return the original
	}
	
	private static Dataset<Row> parseCond( Dataset<Row> ds, String cond) {
		/* We need to parse IF(condition) THEN (expression) [ELSE ELSEIF]
		 * loop. If IF ( true ) then only expressing should be 
		 * parsed and returned conditional column
		 */
		
		Object defaultobj = null;
		boolean isFirst = true;
		
		int otherwiseIndex = cond.indexOf(" OTHERWISE ");
		if (otherwiseIndex != -1) {
			defaultobj = cond.substring(otherwiseIndex + " OTHERWISE ".length(), cond.length()).trim();
			System.out.println("otherwise cond"+ defaultobj.toString());
			cond = cond.substring(1,otherwiseIndex);
		}
			
		while (cond.equals("") == false) {
			cond = cond.trim(); // remove leading trailing whitespaces
			if (cond.startsWith("IF") == true ||  (cond.startsWith("ELSEIF") == true )) {
				Column col= null;
				int indexlen = (cond.startsWith("IF") == true) ? 2 : 6;
				
				int thenIndex = cond.indexOf(" THEN ",indexlen); // it has to come after if
				if (thenIndex < 0) { // IF must have THEN clause
					logger.severe("Format error - \"IF (condition) THEN expression [ELSE ELSEIF]\" ");
					return ds;
				}
				// First check IF condition is OK
				String condition = cond.substring(indexlen, thenIndex).trim(); // Format (column <> value)
				condition = condition.substring(1, condition.length() -1);
				System.out.println("Condition:"+condition);
				
				String[] token = condition.split("\\s+"); // expected 3 members
				System.out.println("Token:"+token[0]+token[1]+token[2]);
				
				ds = ds.withColumn("tempcoldonotreplicatearrahfirst", org.apache.spark.sql.functions.expr(token[2]));
				switch(token[1]) {
					case "==" :
						col = ds.col(token[0]).equalTo(token[2]);
						break;
					case ">" :
						//col = ds.col(token[0]).gt(token[2]);
						col = ds.col(token[0]).gt(ds.col("tempcoldonotreplicatearrahfirst"));
						break;
					case "<" :
						col = ds.col(token[0]).lt(token[2]);
						break;
						
					default:
						logger.severe("Condition error - Supported condition == != >= <= > < ");
				}
				
				String thenexpression="";
				int elseindex = cond.indexOf("ELSE",thenIndex+" THEN ".length());
				
				if (elseindex == -1)
					thenexpression = cond.substring(thenIndex+" THEN ".length()); // take the expression
				else
					thenexpression = cond.substring(thenIndex+" THEN ".length(),elseindex); // take the expression before else
				
				System.out.println("Expression:"+thenexpression);
				
				//ds = ds.selectExpr(thenexpression); // does not work in case cond
				ds = ds.withColumn("tempcoldonotreplicatearrah", org.apache.spark.sql.functions.expr(thenexpression));
				if (isFirst == true)
					//ds = ds.withColumn("TEMPCOL", org.apache.spark.sql.functions.when(col, thenexpression).otherwise(defaultobj));
					ds = ds.withColumn("TEMPCOL", org.apache.spark.sql.functions.
							when(col, ds.col("tempcoldonotreplicatearrah")  ).otherwise(defaultobj));
				else 
					ds = ds.withColumn("TEMPCOL", org.apache.spark.sql.functions.
							when(col, ds.col("tempcoldonotreplicatearrah")).otherwise(ds.col("TEMPCOL")));
				
				ds = ds.drop("tempcoldonotreplicatearrah").drop("tempcoldonotreplicatearrahfirst");
				isFirst = false;
				ds.show(false);
				
				if (elseindex == -1)
					cond ="";
				else
					cond = cond.substring(elseindex,cond.length());
				
			} 
			
		} // end of while
		
		return ds;
	}
}
