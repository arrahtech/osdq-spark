package org.arrah.ml.spark.classifier.service.exception;

public class UndefinedPredictionLabelException extends Exception {
	
	private static final long serialVersionUID = -2889857203184876403L;

	public UndefinedPredictionLabelException(String message) {
		super(message);
	}
}