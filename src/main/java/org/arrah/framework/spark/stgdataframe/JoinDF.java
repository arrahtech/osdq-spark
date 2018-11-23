package org.arrah.framework.spark.stgdataframe;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.arrah.framework.jsonparser.ConditionParser;



public class JoinDF {

	private static final Logger logger = Logger.getLogger(JoinDF.class.getName());
	
	public static Dataset<Row>  joinDF(Dataset<Row>  ldf, Dataset<Row>  rdf, List<ConditionParser> conditions) {

		Dataset<Row>  retDF = null;

		for (ConditionParser c : conditions) {
			retDF = addJoin(ldf, rdf, c);
		}

		return retDF;
	}

	private static Dataset<Row>  addJoin(Dataset<Row>  ldf, Dataset<Row>  rdf, ConditionParser c) {

		String joinType = c.getJoinType();
		Dataset<Row>  filterDF = null;
		
		if (joinType.equalsIgnoreCase("unionall")){ // deprecated to union in 2.0
			return filterDF = ldf.union(rdf);
		}
		
		if (joinType.equalsIgnoreCase("except")){ // deprecated to union in 2.0
			return filterDF = ldf.except(rdf);
		}
		
		if (joinType.equalsIgnoreCase("intersect")){ // deprecated to union in 2.0
			return filterDF = ldf.intersect(rdf);
		}
		
		try {
			filterDF = joincolumnexpr( ldf,  rdf, c);
		} catch (Exception e) {
			logger.severe("Could not join. Error:" +  e.getLocalizedMessage());
			return filterDF;
		}
		
		return filterDF;

	}
	
	private static Dataset<Row> joincolumnexpr(Dataset<Row>  ldf, Dataset<Row>  rdf, ConditionParser c) {
		String condition = c.getCondition();
		String joinType = c.getJoinType();
		String leftColumn = c.getLeftcolumn();
		String rightColumn = c.getRightcolumn();
		String dropColumn = c.getDropcolumn();
		String onconflict = c.getOnconflict();
		
		
		String[] leftCA = leftColumn.split(",");
		String[] rightCA = rightColumn.split(",");
		Column joinColExpr = null;
		
		if (leftCA == null || rightCA == null || leftCA.length < 1 || rightCA.length < 1) {
			logger.severe("Left or Right columns for joins are empty");
			return ldf;
		}
		
		int llen = leftCA.length; int rlen = rightCA.length; 
		int ilen = (llen > rlen) ? rlen :llen;
		
		if (llen !=  rlen) {
			logger.warning("Left or Right columns for joins are not matching. Will ignore non matching indexes");
		}
		
		// check for conflict
		if (onconflict != null && "".equals(onconflict) == false) {
			// onconflict is on add prefix to resolve naming conflict
			String[] leftCols = ldf.columns();
			String[] rightCols = rdf.columns();
			String[] prefix = onconflict.split(",");
			
			List<String> rightColsL = Arrays.asList(rightCols);
			
			for (int i=0; i < leftCols.length; i++) {
				int matchindex = -1;
				int leftI = -1;
				int rightI = -1;
				
				// see if there is a conflict
				if ( (matchindex = rightColsL.indexOf(leftCols[i]) )  >= 0 ) {
					// See if conflict is joinable columns
					if ( (leftI = Arrays.asList(leftCA).indexOf(leftCols[i])) >= 0 )
						leftCA[leftI] = prefix[0]+leftCA[leftI];
					
					if ( (rightI = Arrays.asList(rightCA).indexOf(leftCols[i])) >= 0 )
						rightCA[rightI] = prefix[1]+rightCA[rightI];
					
					leftCols[i] = prefix[0]+leftCols[i];
					rightCols[matchindex] = prefix[1]+rightCols[matchindex];
				}
			}
			
			// now rename
			String[] origCols = ldf.columns();
			for (int i=0; i < origCols.length; i++)
				ldf = ldf.withColumnRenamed(origCols[i],leftCols[i]);
			origCols = rdf.columns();
			for (int i=0; i < origCols.length; i++)
				rdf = rdf.withColumnRenamed(origCols[i],rightCols[i]);
		}
		
		// Now join
		for (int i=0 ; i < ilen; i++) {
			Column joinCol=null;
			
			if (condition.equalsIgnoreCase("equal"))
				joinCol =  ldf.col(leftCA[i]).equalTo(rdf.col(rightCA[i]));
			else if (condition.equalsIgnoreCase("not equal")) 
				joinCol =  ldf.col(leftCA[i]).notEqual(rdf.col(rightCA[i]));
			else if (condition.equalsIgnoreCase("less than equal")) 
				joinCol =  ldf.col(leftCA[i]).leq(rdf.col(rightCA[i]));
			else if (condition.equalsIgnoreCase("greater than equal")) {
				joinCol =  ldf.col(leftCA[i]).geq(rdf.col(rightCA[i]));
			} else {
				logger.warning("This join condition is not supported:" + condition);
			}
			
			if (joinCol != null)
				if (joinColExpr != null)
					joinColExpr = joinColExpr.and(joinCol);
				else
					joinColExpr = joinCol;	
		} 
		
		// Now Join
		Dataset<Row>filterDF = null;
		filterDF = ldf.join(rdf, joinColExpr,joinType);
		
		if(dropColumn !=null && dropColumn.length() >0) 
			filterDF = dropColumns(filterDF,dropColumn);
	
		return filterDF;
		
	}
	
	private static Dataset<Row> dropColumns(Dataset<Row> rdf, String columns) {
		String[] splitCA = columns.split(",");
		if (splitCA == null || splitCA.length < 1 ) {
			logger.warning("Left or Right columns for joins are empty");
			return rdf;
		}
		for (String s:splitCA)
			rdf = rdf.drop(s);
		return rdf;
	}
	
}
