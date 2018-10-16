/*******************************************************************************
 * Copyright (c) 2016 
 *
 *   Any part of code or file can be changed,    
 *   redistributed, modified with the copyright  
 *   information intact .                       
 *  
 * Contributors:
 *         Hareesh Makam
 *******************************************************************************/
package org.arrah.dq.spark.core.datasampler.samplers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class RandomSampler extends RootSampler {
	
	
	public RandomSampler(double fraction){
		this.fraction = fraction;
	}
	

	@Override
	public Dataset<Row> sample(Dataset<Row> df) {
		
		return df.sample(false, fraction);
	}
	

}
