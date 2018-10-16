package org.arrah.framework.spark.stgdataframe;

import java.util.List;
import java.util.logging.Logger;

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

		String condition = c.getCondition();
		String joinType = c.getJoinType();
		String leftColumn = c.getLeftcolumn();
		String rightColumn = c.getRightcolumn();
		String dropColumn = c.getDropcolumn();
		Dataset<Row>  filterDF = null;
		
		if (joinType.equalsIgnoreCase("unionall")){ // deprecated to union in 2.0
			//System.out.println(ldf.count());
			//System.out.println(rdf.count());
			return filterDF = ldf.union(rdf);
			//System.out.println(filterDF.count());
		}

		if (condition.equalsIgnoreCase("equal")){
		
			if(dropColumn !=null && dropColumn.length() >0)
				filterDF = ldf.join(rdf, ldf.col(leftColumn).equalTo(rdf.col(rightColumn)),joinType).drop(rdf.col(rightColumn));
			else
				filterDF = ldf.join(rdf, ldf.col(leftColumn).equalTo(rdf.col(rightColumn)),joinType);
			return filterDF;
		} else if (condition.equalsIgnoreCase("not equal")) {
			if(dropColumn !=null && dropColumn.length() >0)
				filterDF = ldf.join(rdf, ldf.col(leftColumn).notEqual(rdf.col(rightColumn)),joinType).drop(rdf.col(rightColumn));
			else
				filterDF = ldf.join(rdf, ldf.col(leftColumn).notEqual(rdf.col(rightColumn)),joinType);
			return filterDF;
		} else if (condition.equalsIgnoreCase("less than equal")) {
			if(dropColumn !=null && dropColumn.length() >0)
				filterDF = ldf.join(rdf, ldf.col(leftColumn).leq(rdf.col(rightColumn)),joinType).drop(rdf.col(rightColumn));
			else
				filterDF = ldf.join(rdf, ldf.col(leftColumn).leq(rdf.col(rightColumn)),joinType);
			return filterDF;
		} else if (condition.equalsIgnoreCase("greater than equal")) {
			if(dropColumn !=null && dropColumn.length() >0)
				filterDF = ldf.join(rdf, ldf.col(leftColumn).geq(rdf.col(rightColumn)),joinType).drop(rdf.col(rightColumn));
			else
				filterDF = ldf.join(rdf, ldf.col(leftColumn).geq(rdf.col(rightColumn)),joinType);
			return filterDF;
		} else {
			logger.warning("This join condition is not supported:" + condition);
		}
		
		return filterDF;

	}
	
}
