package org.arrah.framework.spark.stgdataframe;

import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.arrah.framework.jsonparser.ConditionParser;
import org.arrah.framework.spark.helper.UDFFile;



public class TransformDF {

	private static final Logger logger = Logger.getLogger(TransformDF.class.getName());
	
	public static Dataset<Row>  transformDF(Dataset<Row>  df, List<ConditionParser> conditions) {

		Dataset<Row>  retDF = df;

		for (ConditionParser c : conditions) {
			retDF = addtransformDF(retDF, c);
			}

		return retDF;
	}
	
	private static Dataset<Row>  addtransformDF(Dataset<Row>  df, ConditionParser conditions) {

		Dataset<Row>  retDF = null;
			String condition = conditions.getCondition();
			
			
			if (condition.equalsIgnoreCase("udf")) {
				// Now call the UDF util
				retDF = UDFFile.udfMappedDF(df,conditions);
				return retDF;
				
			} else if (condition.equalsIgnoreCase("normalise")) {
				// Now call the Normalise util
				String functionName = conditions.getFuncName();
				List<String >colgrp = conditions.getValue();
				Column c = new Column(colgrp.get(0));
				
				if (functionName.equalsIgnoreCase("zeroscore")) {
					
					Row r = df.select(org.apache.spark.sql.functions.max(c),org.apache.spark.sql.functions.min(c)).first();
					Object max=r.get(0); Object min=r.get(1);
					Object diff = Double.parseDouble(max.toString()) - Double.parseDouble(min.toString());
					retDF = df.withColumn(conditions.getColumn(), (c.minus(min).divide(diff)) );
					return retDF;
				}
				if (functionName.equalsIgnoreCase("zscore")) {
					
					Row r = df.select(org.apache.spark.sql.functions.stddev(c),org.apache.spark.sql.functions.mean(c)).first();
					Object stddevv=r.get(0); Object meanv=r.get(1);
					retDF = df.withColumn(conditions.getColumn(), (c.minus(meanv).divide(stddevv)) );
					return retDF;
				}
				
			}
			else {
				logger.warning("This transform condition is not supported:" + condition);
			}

		return df;
	}
	
}
