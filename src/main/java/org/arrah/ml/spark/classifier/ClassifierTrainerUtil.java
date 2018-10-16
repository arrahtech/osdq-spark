package org.arrah.ml.spark.classifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassifierTrainerUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClassifierTrainerUtil.class);

  // Will return profile column values in
  public static HashMap<String, HashMap<String, Object>> showProfile(
      Dataset<Row> df) {

    HashMap<String, HashMap<String, Object>> result = new HashMap<>();
    // DataFrame df = inputBean.getDataFrame();
    String[] colName = df.columns();
    // Loop for each column
    for (int i = 0; i < colName.length; i++) {
      Dataset<Row> coldf = df.select(colName[i]);
      HashMap<String, Object> valueset = new HashMap<String, Object>();
      try {
        long colC = coldf.count(); // count
        valueset.put("count", colC);
        long uniqueC = coldf.distinct().count(); // unique
        valueset.put("unique", uniqueC);
        long nullC = coldf.na().drop().count(); // null count
        valueset.put("nullcount", colC - nullC);

        valueset.put("nullperctange %",
            Math.round((((colC - nullC) * 100) / colC))); // Null Percentage

        long emptyC = coldf.where(coldf.col(colName[i]).equalTo("")).count(); // empty
                                                                              // count
        valueset.put("emptycount", emptyC);

        RelationalGroupedDataset gd = coldf.groupBy(colName[i]);
        Dataset<Row> patterndf = gd.count();
        long patternC = patterndf.where(patterndf.col("count").gt(1)).count();
        valueset.put("pattern", patternC);
        Object mino = coldf.sort().limit(1).toString();
        try {
          Double mind = Double.parseDouble(mino.toString());
          valueset.put("min", mind);
        } catch (Exception e) {
          valueset.put("min", "NA");
        }

        Object maxo = coldf.sort(coldf.col(colName[i]).desc()).limit(1)
            .toString();
        try {
          Double maxd = Double.parseDouble(maxo.toString());
          valueset.put("max", maxd);
        } catch (Exception e) {
          valueset.put("max", "NA");
        }

        result.put(colName[i], valueset);
      } catch (Exception e) {
        System.out.println("Exception :" + e.getLocalizedMessage());
        continue;
      }
    }
    return result;
  }

	
	public static Dataset<Row> split(Dataset<Row> ds, float trainPercentage, Dataset<Row> trainset, Dataset<Row> testset) {
		// Column -> (Value, Integer)
		HashMap<String, HashMap<Object, Long>> columnProfileMap = new HashMap<>();
		String[] cols = ds.columns();
		for (String col : cols) {
			HashMap<Object, Long> columnProfile = new HashMap<>();
			ds.select(col).groupBy(col).count().collectAsList().forEach(r -> {
			  columnProfile.put(r.get(0), r.getLong(1));
			});
			columnProfileMap.put(col, columnProfile);
		}
		LOGGER.info("Total number of columns -> {}", cols.length);
		LOGGER.info("Columns Profile");
		columnProfileMap.forEach((col, profile) -> {
			LOGGER.info("Coulmn Name -> {}", col);
			profile.forEach((value, count) -> {
			  if (value != null && count != null) {
			    LOGGER.info("{} -> {}", value.toString(), count.longValue());
			  }
			});
		});

		// Now go over all rows, by picking a row, checking if picking this will make
		// and column value count to zero.
		// if zero then ignore this row an pick again randomly. Do it until enough test
		// dataset is achieved.
		// Also keep track of random row already checked so far. So that those can be
		// ignored with cheking.

		long totalDsSize = ds.count();

		Random random = new Random(1);

		List<Row> testsetRow = new ArrayList<>();
		List<Row> trainsetRow = new ArrayList<>();
		List<Row> dsRows = ds.collectAsList();
		for(Row t: dsRows) {
		  if (random.nextFloat() > trainPercentage) {
        //fetch a record
        for (String col: cols) {
          LOGGER.info("type -> {}, {}", t.fieldIndex(col), columnProfileMap.get(col).get(String.valueOf(t.fieldIndex(col))));
          long colCountLeft = columnProfileMap.get(col).get(String.valueOf(t.fieldIndex(col)));
          if (colCountLeft - 1 > 0) {
            //put this into test set
            testsetRow.add(t);
            //reduce the col count
            columnProfileMap.get(col).put(t.fieldIndex(col), colCountLeft - 1);
          } else {
            trainsetRow.add(t);
          }
        }
      }
		}
		LOGGER.info("Trainset Size -> {}", trainsetRow.size()*100/totalDsSize);
		LOGGER.info("Testset Size -> {}", testsetRow.size()*100/totalDsSize);
		return null;
	}
}
