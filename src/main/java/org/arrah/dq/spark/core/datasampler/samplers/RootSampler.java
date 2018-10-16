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

public abstract class RootSampler {
	
	public String column_name;
	public abstract Dataset<Row> sample(Dataset<Row> df);
	public double fraction;
	
}
