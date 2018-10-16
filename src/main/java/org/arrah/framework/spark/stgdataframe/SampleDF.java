package org.arrah.framework.spark.stgdataframe;

import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.arrah.dq.spark.core.datasampler.samplers.RandomSampler;
import org.arrah.dq.spark.core.datasampler.samplers.StratifiedSampler;
import org.arrah.framework.jsonparser.ConditionParser;



public class SampleDF {

	private static final Logger logger = Logger.getLogger(SampleDF.class.getName());
	
	public static Dataset<Row>  sampleDF(Dataset<Row>  df, List<ConditionParser> conditions) {

		Dataset<Row>  retDF = df;

		for (ConditionParser c : conditions) {
			retDF = addsampleDF(retDF, c);
			}

		return retDF;
	}
	
	private static Dataset<Row>  addsampleDF(Dataset<Row>  df, ConditionParser conditions) {

			String condition = conditions.getCondition();
			// there maynot be group by as aggr can be done on compete dataset
			if (condition.equalsIgnoreCase("random")) {
				List<String> samplepect = conditions.getValue();
				double samplesize=0.2D; // default 20 %
				try {
					samplesize = Double.parseDouble(samplepect.get(0));
				} catch ( Exception c) {
					samplesize=0.2D;
				}
				// System.out.println(df.count());
				// System.out.println( new RandomSampler(samplesize).sample(df).count());
				return  new RandomSampler(samplesize).sample(df);
			} else if (condition.equalsIgnoreCase("stratified")) {
				List<String> samplepect = conditions.getValue();
				String keyCol = conditions.getColumn();
				
				/*** if first member is > 0.0 and less than < 1.0 this will be default fraction for 
				 * all the keys. Only keys which follow after that number has values will change. If
				 * no values follow then proportionate stratified random sampling.
				 * 
				 * if first member is == 0.0, then it will provide sample only for those keys which follow
				 * if no keys follow it will be error
				 * 
				 * 0.2,col1,0.4,col10,0.3
				 * except col1 and col4 for all columns fraction will be 0.2
				 */
				if ( samplepect == null || samplepect.isEmpty() ) return df;
				
				double samplesize=0.0D; // default 20 %
				try {
					samplesize = Double.parseDouble(samplepect.get(0));
				} catch ( Exception c) {
					samplesize=0.0D;
					System.out.println("Could not parse fraction value:" + c.getLocalizedMessage());
					return df;
				}
				samplepect.remove(0); // now remove the first one
				return  new StratifiedSampler(samplepect, samplesize, keyCol).sample(df);
			} else {
				logger.warning("This sampling condition is not supported:" + condition);
			}

		return df;
	}
}
