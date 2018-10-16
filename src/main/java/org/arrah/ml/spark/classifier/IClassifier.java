package org.arrah.ml.spark.classifier;

@FunctionalInterface
public interface IClassifier {

	/**
	 * @param classifierAlgorithm to be used for classification
	 * @param classificationLabel - the label field to be classified
	 * @param string - the input feature in JSON format
	 * @return
	 */
	public String classify(final String classifierAlgorithm, final String classificationLabel, String string) throws Exception;
	
}
