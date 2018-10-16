package org.arrah.dq.spark.core.dataprocess;


import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;



public class FunctionMap  implements java.io.Serializable, Function<Row,Row> {
/**
* 
*/
private static final long serialVersionUID = 1L;
double _intercept, _weight;
	public FunctionMap(double intercept, double weight) {
		_intercept=intercept;
		_weight = weight;
	}

	public Row call(Row r) throws Exception {
		Double regv = r.getDouble(1);
		if (r.get(0) == null && regv != null) {
		double newVal = _intercept + _weight * regv;
		return RowFactory.create(newVal, regv, r.getDouble(2));
		} else
		return r;
	}

}