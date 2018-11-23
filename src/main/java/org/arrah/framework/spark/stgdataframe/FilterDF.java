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
		
		} else if (condition.equalsIgnoreCase("LIKE")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).like(newValue.get(0).toString()));
		
		} else if (condition.equalsIgnoreCase("RLIKE")) {
			
			filterDF = df.where(df.col(c.getColumn()).cast(dt).rlike(newValue.get(0).toString()));
		
		}  else if (condition.equalsIgnoreCase("DROPDUPLICATES")) {
			if (newValue == null || newValue.isEmpty() == true)
				filterDF = df.dropDuplicates();
			else {
				String[] str = new String[newValue.size()];
				filterDF = df.dropDuplicates(newValue.toArray(str));
			}
		} else if (condition.equalsIgnoreCase("SELECTEXPR")) {
			
				String[] str = new String[newValue.size()];
				filterDF = df.selectExpr(newValue.toArray(str));
		}
		else if (condition.equalsIgnoreCase("EXPRESSION")) {
			
			filterDF = df.where(newValue.get(0).toString());
		
		}  else if (condition.equalsIgnoreCase("COLNAMEFROMFILE")) {
			
			String[] colNames = df.columns();
			filterDF = df;
			int i=0;
			int outlen = newValue.size();
			for (String colName:colNames) {
				if (i < outlen)
					filterDF = filterDF.withColumnRenamed(colName, newValue.get(i++).toString());
				else
					break; // Not enough so break it
			}
			return filterDF;
		
		}
		
		else {
			logger.warning("This filter condition is not supported:" + condition);
		}
		return filterDF;

	}
	
}
