package org.arrah.framework.spark.stgdataframe;

import java.util.List;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.arrah.framework.jsonparser.ConditionParser;
import org.arrah.framework.spark.helper.SparkHelper;
import org.arrah.framework.spark.helper.UDFilter;


public class FilterDF {

	private static final Logger logger = Logger.getLogger(FilterDF.class.getName());
	
	public static Dataset<Row>  filterDF(Dataset<Row>  df, List<ConditionParser> conditions) {

		Dataset<Row>  retDF = df;

		for (ConditionParser c : conditions) {
			retDF = addFilter(retDF, c);
			}

		return retDF;
	}

	private static Dataset<Row>  addFilter(Dataset<Row>  df, ConditionParser c) {

		String condition = c.getCondition();
		Dataset<Row>  filterDF = null;

		DataType dt = SparkHelper.getDatatype(c.getDatatype()); 
		List<? extends Object> newValue = UDFilter.parseForValues(c.getValue());
		//System.out.println(newValue.get(0));
		
		if (condition.equalsIgnoreCase("GREATEREQUAL")) {
			filterDF = df.where(df.col(c.getColumn()).cast(dt).geq(newValue.get(0)));
		
		} else if (condition.equalsIgnoreCase("LESSEQUAL")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).leq(newValue.get(0)));
		
		} else if (condition.equalsIgnoreCase("IN")) {
		
		//	filterDF = df.where(df.col(c.getColumn()).isin(scala.collection.JavaConverters.asScalaIteratorConverter(c.getValue().iterator()).asScala().toSeq()));
			filterDF = df.where(df.col(c.getColumn()).cast(dt).isin(newValue.toArray()));
			
		} else if (condition.equalsIgnoreCase("EQUALS")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).equalTo(newValue.get(0)));
		
		}else if (condition.equalsIgnoreCase("NOTEQUALS")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).notEqual(newValue.get(0)));
		
		} else if (condition.equalsIgnoreCase("GREATER")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).gt(newValue.get(0)));
		
		} else if (condition.equalsIgnoreCase("LESS")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).lt(newValue.get(0)));
		
		} else {
			logger.warning("This filter condition is not supported:" + condition);
		}
		return filterDF;

	}
	
}
