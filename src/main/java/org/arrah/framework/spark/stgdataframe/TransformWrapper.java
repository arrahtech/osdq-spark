package org.arrah.framework.spark.stgdataframe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.arrah.framework.jsonparser.ConditionParser;
import org.arrah.framework.jsonparser.TransformationParser;
import org.arrah.framework.spark.helper.UDFilter;

import scala.collection.JavaConverters;

public class TransformWrapper {

	private static final Logger logger = Logger.getLogger(TransformWrapper.class.getName());

	public static Dataset<Row>  applyTransform(TransformationParser t, HashMap<String, Dataset<Row> > dfMap) {

		String tfType = t.getType();

		List<ConditionParser> conditions = t.getConditions();

		if (tfType.equalsIgnoreCase("filter")) {

			return FilterDF.filterDF(dfMap.get(t.getSource()), conditions);

		} else if (tfType.equalsIgnoreCase("join")) {

			String[] sources = t.getSource().split(",");

			return JoinDF.joinDF(dfMap.get(sources[0]), dfMap.get(sources[1]), conditions);

		} else if (tfType.equalsIgnoreCase("query")) {

			String sources = t.getSource();
			
			return sqlDF(dfMap.get(sources), conditions,t.getName());

		} else if (tfType.equalsIgnoreCase("aggregate")) {

			String sources = t.getSource();
			
			return aggrDF(dfMap.get(sources), conditions);

		} else if (tfType.equalsIgnoreCase("enrichment")) {

			String sources = t.getSource();
			
			return EnrichDF.enrichDF(dfMap.get(sources), conditions);

		} else if (tfType.equalsIgnoreCase("transformation")) {

			String sources = t.getSource();
			
			return TransformDF.transformDF(dfMap.get(sources), conditions);

		}   else if (tfType.equalsIgnoreCase("sampling")) {

			String sources = t.getSource();
			
			return SampleDF.sampleDF(dfMap.get(sources), conditions);

		}
		else if (tfType.equalsIgnoreCase("profile")) {

			String sources = t.getSource();
			
			Hashtable<String,Hashtable<String,Object>> hashVal =  profileDF(dfMap.get(sources), conditions);
			Set<String> keys = hashVal.keySet();
		    Iterator<String> itr = keys.iterator();
		 
		    //Displaying Key and value pairs
		    while (itr.hasNext()) { 
		       // Getting Key
		       String strkey = itr.next();
		       System.out.println("\nColumn:"+strkey);
		       Hashtable<String,Object> hash2val = hashVal.get(strkey);
		       
		       Set<String> keysnew = hash2val.keySet();
			    Iterator<String> itrnew = keysnew.iterator();
			 
			    //Displaying Key and value pairs
			    while (itrnew.hasNext()) { 
			       // Getting Key
			       String newvalStr = itrnew.next();
			       System.out.println("Key: "+newvalStr+" Value: "+hash2val.get(newvalStr).toString());
			    }

		    }
			return dfMap.get(sources);

		} else {
			System.out.println("This type is not supported:" + tfType);
			logger.warning("This type is not supported:" + tfType);
		}

		return null;

	}

	// we have to functionality so that it can take from file or inline json
	private static Dataset<Row>  sqlDF(Dataset<Row>  dataFrame, List<ConditionParser> conditions,String tableName) {

		
		Dataset<Row>  retDF = dataFrame;
		
		for (ConditionParser c : conditions)  {
			retDF.createOrReplaceTempView(tableName);
			String fileorinline= c.getCondition();
			if (fileorinline == null || "".equals(fileorinline) || fileorinline.toLowerCase().startsWith("file") == false) 
				retDF = retDF.sqlContext().sql(c.getSql());
			else { // take sql from file
				String filename= fileorinline.split(",")[1];
				String sqlstr = UDFilter.filetoSQL(filename);
				//System.out.println("SQL:" + sqlstr);
				retDF = retDF.sqlContext().sql(sqlstr);
			}
		}
		
		return retDF;
		
	}
	
	private static Dataset<Row>  aggrDF(Dataset<Row>  df, List<ConditionParser> conditions) {

		Dataset<Row>  retDF = null;
		for (ConditionParser c : conditions) {
			String condition = c.getCondition();
			// there maynot be group by as aggr can be done on compete dataset
			if (condition.equalsIgnoreCase("GROUPBY")) {
				List<String >colgrp = c.getValue();
				List<Column> grpcol = new ArrayList<Column> ();
				
				for( String s: colgrp)
					grpcol.add( new Column(s) );
				// Now get aggregate condition Map
				String aggrCond = c.getAggrcondition();
				String[] aggrCA = aggrCond.split(",");
				Map<String,String> condMap = new HashMap<String,String>();
				for (int i=0; i < aggrCA.length; i=i+2)
					// TO match sql type sum(colname) change key value
					//condMap.put(aggrCA[i], aggrCA[i+1]); 
					condMap.put(aggrCA[i+1], aggrCA[i]); 
				
				
				// aggre can be only min,max,sum,count,avg
				//System.out.println(grpcol.toString());
				retDF = df.groupBy(JavaConverters.asScalaBufferConverter(grpcol).asScala()).agg(condMap);
			}
			
		}
		

		return retDF;
	}
		
	// Will return profile column values in 
	private static Hashtable<String,Hashtable<String,Object>> profileDF(Dataset<Row>  df, List<ConditionParser> conditions) {

		Hashtable<String,Hashtable<String,Object>> result = new Hashtable<String,Hashtable<String,Object>>();
		// DataFrame df = inputBean.getDataFrame();
		String[] colName = df.columns();
		// Loop for each column
		for (int i=0; i < colName.length; i++) {
			Dataset<Row>  coldf = df.select(colName[i]);
			Hashtable<String,Object> valueset = new Hashtable<String,Object>();
			try {
				long colC = coldf.count(); // count
				valueset.put("count", colC);
				long uniqueC = coldf.distinct().count(); // unique
				valueset.put("unique", uniqueC);
				long nullC = coldf.na().drop().count(); // null count
				valueset.put("nullcount",colC - nullC);
				
				valueset.put("nullperctange %",Math.round((((colC - nullC)*100)/colC))); // Null Percentage
				
				long emptyC = coldf.where(coldf.col(colName[i]).equalTo("")).count(); // empty count
				valueset.put("emptycount",emptyC);
				
				
				RelationalGroupedDataset gd = coldf.groupBy(colName[i]);
				Dataset<Row>  patterndf = gd.count();
				long patternC = patterndf.where(patterndf.col("count").gt(1)).count();
				valueset.put("pattern", patternC);
				Object mino = coldf.sort().limit(1).toString();
				try {
					Double mind = Double.parseDouble(mino.toString());
					valueset.put("min", mind);
				} catch (Exception e) {
					valueset.put("min", "NA");
				}
				
				Object maxo = coldf.sort(coldf.col(colName[i]).desc()).limit(1).toString();
				try {
					Double maxd = Double.parseDouble(maxo.toString());
					valueset.put("max", maxd);
				} catch (Exception e) {
					valueset.put("max", "NA");
				}
				
				result.put(colName[i], valueset);
			} catch (Exception e) {
				System.out.println("Exception :"+e.getLocalizedMessage());
				continue;
			}	
		}
		return result;
	}
	
}
