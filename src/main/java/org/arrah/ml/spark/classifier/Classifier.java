package org.arrah.ml.spark.classifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.arrah.ml.spark.classifier.service.exception.UndefinedPredictionLabelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A classifier front-end for Classifier
 * 
 */
public class Classifier implements IClassifier {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(Classifier.class);

  private final Properties classifierProperties = new Properties();
  private final SparkSession spark;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final HashMap<String, PipelineModel> hashMap = new HashMap<>();

  private static IClassifier classifier;

  static {
    try {
      classifier = new Classifier();
    } catch (IOException e) {
      throw new RuntimeException("Error creating RandomForestClassifier", e);
    }
  }

  public static IClassifier getInstance() {
    return classifier;
  }

  private SparkSession getSpark() {
    LOGGER.info("Initializing spark configuration with `{}` master",
        classifierProperties.getProperty("spark.master"));
    SparkConf conf = new SparkConf().setAppName("spark-rf")
        .setMaster(classifierProperties.getProperty("spark.master"))
        .set("spark.driver.host",
            classifierProperties.getProperty("spark.driver.host"))
        .set("spark.driver.memory",
            classifierProperties.getProperty("spark.driver.memory"))
        .set("spark.executor.memory", "6g")
        .set("spark.ui.showConsoleProgress", "false");
    LOGGER.info("Getting hold of spark session");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    LOGGER.info("Getting hold of spark session - [OK]");
    return spark;
  }

  private Pattern modelNamePattern = Pattern.compile("(.*?)-");

  private Classifier() throws IOException {
    classifierProperties.load(Classifier.class.getClassLoader()
        .getResourceAsStream("classifier.properties"));
    objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES,
        true);

    LOGGER.info("Getting hold of spark session");
    spark = getSpark();
    LOGGER.info("Getting hold of spark session - [OK]");

    String modelPath = classifierProperties.get("model.path").toString();
    LOGGER.info("Getting hold of pre-trained models for all algorithms");
    try (Stream<Path> algorithm_dir_paths = Files.list(Paths.get(modelPath))) {
      // For each algorithm
      algorithm_dir_paths.forEach(algorithm_dir_path -> {
        String classifierAlgorithm = Paths.get(modelPath).relativize(algorithm_dir_path).toString();
        LOGGER.info("Loading all models under '{}'",
            algorithm_dir_path.toAbsolutePath().toString());
        try (Stream<Path> model_paths = Files.list(algorithm_dir_path)) {
          // for each model
          model_paths.forEach(model_path -> {
                Path mp = algorithm_dir_path.relativize(model_path);
                LOGGER.info("model found '{}'",
                    mp);
                Matcher matcher = modelNamePattern
                    .matcher(mp.toString());
                if (matcher.find()) {
                  String classificationLabel = matcher.group().replaceAll("-", "");
                  LOGGER.info("Model to be loaded for label - {}", classificationLabel);
                  //Keeping algorithmName and lalbeName as a key
                  hashMap.put(classifierAlgorithm.toLowerCase() + "_" + classificationLabel.toLowerCase(),
                      PipelineModel.load(model_path.toString()));
                  LOGGER.info("Model to be loaded for label - {} [OK]", classificationLabel);
                } else {
                  LOGGER.warn("Model path {} doesn't follow the convention");
                }
              });
        } catch (IOException ioe) {
          LOGGER.error("Error opening model file");
        }
      });
    }
    LOGGER.info("Getting hold of pre-trained model for {} algorithm - [OK]");
  }

  public String classify(final String classifierAlgorithm,
      final String classificationLabel, final String input) throws Exception {
    LOGGER.info("Preparing dataset for given input -{}", input);

    ArrayNode arrayNode = (ArrayNode) objectMapper.readTree(input);

    // get hold of schema from first element
    ArrayList<StructField> stfList = new ArrayList<>();
    ObjectNode firstElement = (ObjectNode) arrayNode.get(0);
    firstElement.fields().forEachRemaining(e -> {
      stfList.add(new StructField(e.getKey(), DataTypes.StringType, false,
          Metadata.empty()));
    });
    StructType st = new StructType(stfList.toArray(new StructField[] {}));

    // load all rows
    List<Row> rows = new ArrayList<>(arrayNode.size());
    arrayNode.forEach(t -> {
      ObjectNode rootObject = (ObjectNode) t;
      ArrayList<String> values = new ArrayList<>();
      rootObject.fields().forEachRemaining(e -> {
        stfList.add(new StructField(e.getKey(), DataTypes.StringType, false,
            Metadata.empty()));
        values.add(e.getValue().asText());
      });
      Row row = new GenericRow(values.toArray(new String[] {}));
      rows.add(row);
    });
    Dataset<Row> df = spark.createDataFrame(rows, st);
    LOGGER.info("Input Received");
    df.show();
    List<HashMap<String, String>> predictionList = new ArrayList<>();
    Dataset<Row> pred = null;
    //Get model from map
    PipelineModel pipelineModel = hashMap
        .get(classifierAlgorithm.toLowerCase() + "_" + classificationLabel.toLowerCase());
    if (pipelineModel != null) {
      pred = pipelineModel.transform(df);
    } else {
      throw new UndefinedPredictionLabelException("Invalid prediction type");
    }
    try {
	    pred.select("predictedLabel", "probability").collectAsList().forEach(r1 -> {
	      HashMap<String, String> prediction = new HashMap<>(1);
	      prediction.put("predictedLabel", r1.getString(0));
	      // TODO: currently MLPC doesn;t give probability. will be available spark
	      // 2.3.0
	      DenseVector dv = (DenseVector) r1.getAs(1);
	      prediction.put("probability", String.valueOf(dv.values()[dv.argmax()]));
	      predictionList.add(prediction);
	    });
    } catch (Exception e) {
    	pred.select("predictedLabel").collectAsList().forEach(r1 -> {
  	      HashMap<String, String> prediction = new HashMap<>(1);
  	      prediction.put("predictedLabel", r1.getString(0));
  	      // TODO: currently MLPC doesn;t give probability. will be available spark
  	      // 2.3.0
  	      predictionList.add(prediction);
  	    });
    	return objectMapper.writeValueAsString(predictionList);
    }
    return objectMapper.writeValueAsString(predictionList);
  }
}
