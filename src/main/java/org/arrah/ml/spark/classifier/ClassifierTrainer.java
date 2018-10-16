package org.arrah.ml.spark.classifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassifierTrainer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClassifierTrainer.class);

	private final Properties classifierProperties = new Properties();

	private SparkSession spark;

	public ClassifierTrainer() throws IOException {
		classifierProperties
				.load(ClassifierTrainer.class.getClassLoader().getResourceAsStream("classifier.properties"));
		spark = getSpark();
	}

	private SparkSession getSpark() {
		LOGGER.info("Initializing spark configuration with `{}` master",
				classifierProperties.getProperty("spark.master"));
		SparkConf conf = new SparkConf().setAppName("spark-rf")
				.setMaster(classifierProperties.getProperty("spark.master"))
				.set("spark.driver.host", classifierProperties.getProperty("spark.driver.host"))
				.set("spark.driver.memory", classifierProperties.getProperty("spark.driver.memory"))
				.set("spark.executor.memory", "6g")
				.set("spark.ui.showConsoleProgress", "false");
		LOGGER.info("Getting hold of spark session");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		LOGGER.info("Getting hold of spark session - [OK]");
		return spark;
	}

	public void train(Dataset<Row> trainDataset, Dataset<Row> testDataset, final String[] featureCols,
			final String labelCol, StringIndexerModel labelIndexer) throws IOException {

		ArrayList<PipelineStage> pipelinesStages = new ArrayList<PipelineStage>();

		String[] featureColsIndexed = new String[featureCols.length];
		for (int featureColIndex = 0; featureColIndex < featureCols.length; featureColIndex++) {
			pipelinesStages.add(new StringIndexer()
					.setInputCol(featureCols[featureColIndex]).setOutputCol("Indexed" + featureCols[featureColIndex]));
			featureColsIndexed[featureColIndex] = "Indexed" + featureCols[featureColIndex];
		}

		// Set the input columns from which we are supposed to read the values
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(featureColsIndexed)
				.setOutputCol("features");
		pipelinesStages.add(assembler);

		// Initialize Classifier Algorithm
		String classifierAlgorithm = classifierProperties.getProperty("classifier.algorithm");
		LOGGER.info("Initializing {} Algorithm for classification", classifierAlgorithm);
		if (classifierAlgorithm.equalsIgnoreCase("RANDOM_FOREST")) {
			RandomForestClassifier rf = new RandomForestClassifier().setLabelCol(labelCol + "Label")
					.setFeaturesCol("features").setMaxBins(1500).setNumTrees(8);
			pipelinesStages.add(rf);
		} else if (classifierAlgorithm.equalsIgnoreCase("MULTILEVEL_PERCEPTRON")) {
			// TODO: To test with different layer sizes, what is block size?
			// Get tFileshe distinct class in predicted label
			int classCount = (int) trainDataset.select(labelCol).distinct().count();
			MultilayerPerceptronClassifier multilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
					.setLayers(new int[] { featureCols.length, 10, 10, classCount }).setBlockSize(128).setSeed(123456789L)
					.setMaxIter(100);
			multilayerPerceptronClassifier.setLabelCol(labelCol + "Label").setFeaturesCol("features");
			pipelinesStages.add(multilayerPerceptronClassifier);
		} else {
			throw new RuntimeException("Unsupported Algorithm");
		}
		LOGGER.info("Initializing {} Algorithm for classification - [OK]", classifierAlgorithm);

		// Convert indexed labels back to original labels.
		IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
				.setLabels(labelIndexer.labels());
		pipelinesStages.add(labelConverter);

		PipelineStage[] pipelinesStageArr = new PipelineStage[pipelinesStages.size()];
		// Chain indexers and forest in a Pipeline
		Pipeline pipeline = new Pipeline().setStages(pipelinesStages.toArray(pipelinesStageArr));

		LOGGER.info("Training dataset");
		// Train model. This also runs the indexers.
		PipelineModel model = pipeline.fit(trainDataset);
		LOGGER.info("Training dataset - [OK]");

		LOGGER.info("Predicting on test dataset");
		// Make predictions.
		Dataset<Row> predictions = model.transform(testDataset);
		LOGGER.info("Predicting on test dataset - [OK]");

		int predictionRows = (int) predictions.count();
		LOGGER.info("Total prediction row -> {}", predictionRows);

		// Select example rows to display.
		predictions.select("predictedLabel", "prediction", labelCol, labelCol + "Label").show(5);

		// Select (prediction, true label) and compute test error
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol(labelCol + "Label").setPredictionCol("prediction").setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		LOGGER.info("Accuracy = " + accuracy);

		// Get evaluation metrics.
		MulticlassMetrics metrics = new MulticlassMetrics(
				predictions.select(predictions.col("prediction"), predictions.col(labelCol + "Label")));

		LOGGER.info("{}", metrics.accuracy());
		LOGGER.info("Weighted Preceision -> {}", metrics.weightedPrecision());
		LOGGER.info("Weighted Recall -> {}", metrics.weightedRecall());
		LOGGER.info("Weighted F1 Score -> {}", metrics.weightedFMeasure());
		LOGGER.info("Weighted True Positive Rate -> {}", metrics.weightedTruePositiveRate());
		LOGGER.info("Weighted False Positive Rate -> {}", metrics.weightedFalsePositiveRate());

		saveModel(model, classifierAlgorithm, labelCol);

		LOGGER.info("*** Training completed ***");

	}
	
	private void saveModel(final PipelineModel model, final String classifierAlgorithm, final String labelCol) throws IOException {
		//IMP: please make sure you do not change model directory naming convention without modifying the code where model is loaded
		String modelFileName = classifierProperties.get("model.path") + "/" + classifierAlgorithm + "/" + labelCol
				+ "-model";
		
		LOGGER.info("Creating algorithm specific directory");
		if (!Files.exists(Paths.get(classifierProperties.get("model.path") + "/"))) {
			LOGGER.info("Algorithm directory '{}' doesn't exists, creating it for first time", classifierAlgorithm);
			Files.createDirectory(Paths.get(classifierProperties.get("model.path") + "/"));
			LOGGER.info("Algorithm directory '{}' doesn't exists, creating it for first time - [OK]", classifierAlgorithm);
		} else {
			LOGGER.info("Algorithm directory '{}' already exists, continue..", classifierAlgorithm);
		}
		
		LOGGER.info("Saving model file, {}", modelFileName);
		model.write().overwrite().save(modelFileName);
		LOGGER.info("Saving model file, {} - [OK]", modelFileName);
	}

	public void train(final String[] featureCols) throws IOException {

		LOGGER.info("Loading dataset from {}", classifierProperties.getProperty("dataset.path"));
		Dataset<Row> df = spark.read().option("header", true).option("mode", "DROPMALFORMED").option("delimiter", 
		    classifierProperties.getProperty("dataset.delimeter", ","))
				.csv(classifierProperties.getProperty("dataset.path"));
		LOGGER.info("Loading dataset - [OK]");

		LOGGER.info("Splitting dataset");
		double trainPercentage = Double.parseDouble(classifierProperties.getProperty("dataset.split.train"));
		double testPercentage = Double.parseDouble(classifierProperties.getProperty("dataset.split.test"));

		LOGGER.info("Full Set Size -> {}", df.count());
		// Replace any null with string value NULLVALUE
		df = df.na().fill("NULLVALUE");

		String labelColsList = classifierProperties.getProperty("classifier.labelcols");
		String[] labelCols = null;
		if ("*".equals(labelColsList)) {
			labelCols = df.columns();
			LOGGER.info("All colums will be predicted -> {}", Arrays.asList(labelCols));
		} else {
			labelCols = labelColsList.split(",");
			LOGGER.info("Given colums will be predicted -> {}", Arrays.asList(labelCols));
		}

		Set<String> hashSet = new HashSet<>(Arrays.asList(featureCols));
		for (String labelCol : labelCols) {
			labelCol = labelCol.trim();
			LOGGER.info("Processing labelCol -> {}", labelCol);
			if (hashSet.contains(labelCol)) {
				LOGGER.info("Skipping because feature Column");
				continue;
			}

			long labelUniqueCount = df.select(labelCol).distinct().count();

			int labelUniqueCountLowMark = Integer.parseInt(classifierProperties.getProperty("label.unique.low", "1"));
			int labelUniqueCountHighMark = Integer.parseInt(classifierProperties.getProperty("label.unique.high", "25000"));
			if (labelUniqueCount <= labelUniqueCountLowMark || labelUniqueCount > labelUniqueCountHighMark) {
				LOGGER.info("Skipping because too low or high unique Column");
				continue;
			}

			// Index labels, adding metadata to the label column.
			// Fit on whole dataset to include all labels in index.
			LOGGER.info("Indexing Label '{}'", labelCol);
			StringIndexerModel labelIndexer = new StringIndexer().setInputCol(labelCol).setOutputCol(labelCol + "Label")
					.fit(df);
			df = labelIndexer.transform(df);
			LOGGER.info("Indexing Label '{}' - [OK]", labelCol);
			LOGGER.info("Splitting dataset");
			Dataset<Row>[] splits = df.randomSplit(new double[] { trainPercentage, testPercentage }, 1);
			LOGGER.info("Splitting dataset - [OK]");

			LOGGER.info("Training Set Size -> {}", splits[0].count());
			LOGGER.info("Test Set Size -> {}", splits[1].count());

			// /*
			// * Keeping full set as training set because RF algorithm
			// * should see all categorical values
			// * Though its not required for MLPC
			// */
			Dataset<Row> trainDataset = df;
			Dataset<Row> testDataset = splits[1];

			LOGGER.info("Training Set Size (Full set)-> {}", trainDataset.count());
			LOGGER.info("Test Set Size -> {}", testDataset.count());

			LOGGER.info("Training for label '{}'", labelCol);
			train(trainDataset, testDataset, featureCols, labelCol, labelIndexer);
			LOGGER.info("Training for label '{}' - [OK]", labelCol);
		}
	}

	public static void main(String[] argv) throws IOException {

	  ClassifierTrainer randomForestTrainer = new ClassifierTrainer();
		String[] featureCols = randomForestTrainer.classifierProperties.getProperty("classifier.featurecols", "")
				.split(",");
		LOGGER.info("Feature Cols -> {}", Arrays.asList(featureCols)); 
		String commandLine = System.getProperty("classifier.featurecols");
		if (commandLine != null) {
			featureCols = commandLine.split(",");
		}
		randomForestTrainer.train(featureCols);
	}

}