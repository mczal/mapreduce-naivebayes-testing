package reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import reducer.utils.ClassContainerRed;
import reducer.utils.ClassPriorRed;
import reducer.utils.ConfusionMatrix;

/**
 * Created by mczal on 22/02/17.
 */
public class TestingReducer extends Reducer<Text, Text, Text, Text> {

  private static final String HDFS_AUTHORITY = "hdfs://localhost:9000";
  private ClassContainerRed classContainerRed = new ClassContainerRed();
  private String[] classSplitConf;
  private String[] attrSplitConf;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
//    for (Text value : values) {
//      context.write(key, value);
//    }
    ClassPriorRed classPriorRed = classContainerRed.getClassPriorRedHashMap()
        .get(key.toString().trim());
    if (classPriorRed == null) {
      throw new IllegalArgumentException(
          "\nNULL EXCEPTION ON TestingReducer.class for class: " + key.toString().trim() + "\n"
              + "Available: " + classPriorRed.toString());
    }
    ConfusionMatrix confusionMatrix = new ConfusionMatrix(classPriorRed.getValues().size(),
        classPriorRed);

    String outKey = "@" + key.toString().trim() + System.lineSeparator();
    String outVal = "";
    for (Text i : values) {
      String s = i.toString().trim();
      String[] splitter = s.split("\\|");
      String actual = splitter[2].split("=")[1].trim();
      String predicted = splitter[0].split("=")[1].trim();
      String percentage = splitter[1].split("=")[1].trim();
      try {
        int predIndex = confusionMatrix.getInfo().get(predicted);
        int actIndex = confusionMatrix.getInfo().get(actual);
        confusionMatrix.getMatrix()[actIndex][predIndex]++;
//        outKey += actual + "|" + predicted + "|" + percentage + System.lineSeparator();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println(e.toString());
      }
    }
    outKey = separateBetConfusion(outKey);
    outKey += System.lineSeparator();
    outKey += confusionMatrix.stringPrintedMatrix() + System.lineSeparator();
    outKey += separateBetConfusion(outVal);

    /**
     * Calculate ErrorRates
     * */
    /**
     *  Accuracy
     * */
    int dividend = 0;
    int divisor = 0;
    for (int i = 0; i < confusionMatrix.getMatrix().length; i++) {
      for (int j = 0; j < confusionMatrix.getMatrix().length; j++) {
        if (i == j) {
          dividend += confusionMatrix.getMatrix()[i][j];
        }
        divisor += confusionMatrix.getMatrix()[i][j];
      }
    }
    outVal += System.lineSeparator();
    String accuracyOperator = dividend + "/" + divisor;
    double accuracyResult = (dividend * 1.0) / (divisor * 1.0);
    outVal += "****" + System.lineSeparator() + "Accuracy:" + System.lineSeparator();
    outVal += accuracyOperator + " = " + accuracyResult;
    outVal += System.lineSeparator();

    for (Entry<String, Integer> stringIntegerEntry : confusionMatrix.getInfo().entrySet()) {
      String className = stringIntegerEntry.getKey();
      Integer index = stringIntegerEntry.getValue();
      outVal += "*For Value = " + className + System.lineSeparator();
      int currTP = confusionMatrix.getMatrix()[index][index];
      int currFP = 0;
      int currFN = 0;
      for (int i = 0; i < confusionMatrix.getMatrix().length; i++) {
        if (i != index) {
          currFP += confusionMatrix.getMatrix()[i][index];
          currFN += confusionMatrix.getMatrix()[index][i];
        }
      }
      /**
       * Precision
       * */
      outVal += "Precision:" + System.lineSeparator();
      String precisionOperation = currTP + " / " + currTP + " + " + currFP;
      double precisionResult = currTP * 1.0 / (currTP + currFP) * 1.0;
      outVal += "-> " + precisionOperation + " = " + precisionResult + System.lineSeparator();
      /**
       * Recall
       * */
      outVal += "Recall:" + System.lineSeparator();
      String recallOperation = currTP + " / " + currTP + " + " + currFN;
      double recallResult = currTP * 1.0 / (currTP + currFN) * 1.0;
      outVal += "-> " + recallOperation + " = " + recallResult + System.lineSeparator();

      /**
       * F Measure
       * */
      double alpha = (2.0 * precisionResult * recallResult) / (precisionResult + recallResult);
      String fMeasureOperation =
          "{1 / { " + String.format("%.2f", alpha) + " {1 / P}+(1- " + String
              .format("%.2f", alpha) + " ) {1 / R} }}";
      double fMeasureResult = 1 /
          ((alpha * (1 / precisionResult)) + ((1 - alpha) * (1 / recallResult)));
      outVal += "F-Measure:" + System.lineSeparator();
      outVal += "-> " + fMeasureOperation + " = " + fMeasureResult + System.lineSeparator();
    }
    outVal += "****";
    /**
     * End Of Calculate ErrorRates
     * */

    context.write(new Text(outKey), new Text(outVal));
  }

  private String separateBetConfusion(String s) {
    int length = 4;
    for (int i = 0; i < length; i++) {
      s += "#";
    }
    return s;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String outputModelPath = conf.get("outputModelPath");

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(HDFS_AUTHORITY + outputModelPath);
    FileStatus[] fileStatuses = fs.listStatus(path);
    for (int i = 0; i < fileStatuses.length; i++) {
      BufferedReader br = new BufferedReader(
          new InputStreamReader(fs.open(fileStatuses[i].getPath())));
      while (br.ready()) {
        String currLine = br.readLine().toLowerCase();
        String splitterBetweenType[] = currLine.split("\\|");
        String currType = splitterBetweenType[splitterBetweenType.length - 1].trim();
        String[] splitterDetail = splitterBetweenType[0].trim().split(",");
        if (currType.trim().equalsIgnoreCase("CLASS")) {
          ClassPriorRed classPriorRed = classContainerRed.getClassPriorRedHashMap()
              .get(splitterDetail[0].trim());
          if (classPriorRed == null) {
            classPriorRed = new ClassPriorRed(splitterDetail[0].trim());
            classContainerRed.getClassPriorRedHashMap()
                .put(splitterDetail[0].trim(), classPriorRed);
          }
          classPriorRed.getValues().add(splitterDetail[1].trim());
        }
      }
    }
  }
}
