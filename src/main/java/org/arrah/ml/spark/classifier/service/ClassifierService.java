package org.arrah.ml.spark.classifier.service;

import static spark.Spark.exception;
import static spark.Spark.post;

import java.io.IOException;
import java.util.Properties;

import org.arrah.ml.spark.classifier.Classifier;
import org.arrah.ml.spark.classifier.IClassifier;
import org.arrah.ml.spark.classifier.service.exception.UndefinedPredictionLabelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RESTFul service API for classification job
 *
 */
public class ClassifierService {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClassifierService.class);

	private final Properties classifierProperties = new Properties();
	private final IClassifier classifier;

	private ClassifierService() throws IOException {
		classifierProperties
				.load(ClassifierService.class.getClassLoader().getResourceAsStream("classifier.properties"));
		classifier = Classifier.getInstance();
		
	}
	
	private void registerServiceRoutes() {
		post("/classify/:algo_name/:label", (req, res) -> {
			String reqBody = req.body();
			LOGGER.info("Received {} classify request for {} -> {}", req.params(":algo_name"), req.params(":label"), reqBody);
			res.type("application/json");
			return classifier.classify(req.params(":algo_name"), req.params(":label"), reqBody);
		});
	}
	
	private void registerServiceException() {
		exception(UndefinedPredictionLabelException.class, (exception, request, response) -> {
			LOGGER.warn(exception.getMessage());
			response.status(400);
			response.type("text/plain");
			response.body(exception.getMessage());
		});
		exception(Exception.class, (exception, request, response) -> {
			LOGGER.warn(exception.getMessage());
			response.status(400);
			response.type("text/plain");
			response.body("couldn't process the request, check if input is right");
		});
	}

	public static void main(String[] argv) throws IOException {
		LOGGER.info("Starting Classifier Serivie");
		ClassifierService classifierService = new ClassifierService();
		LOGGER.info("Starting Classifier Serivie - [OK]");
		
		LOGGER.info("Registering service routes");
		classifierService.registerServiceRoutes();
		LOGGER.info("Registering service routes - [OK]");
		
		LOGGER.info("Registering service exceptions");
		classifierService.registerServiceException();
		LOGGER.info("Registering service exceptions - [OK]");
	}
}
