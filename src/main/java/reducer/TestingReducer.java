package reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import mapper.utils.classp.ClassContainer;
import mapper.utils.classp.ClassPrior;
import mapper.utils.classp.ClassPriorDetail;
import mapper.utils.predictor.PredictorContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
      int predIndex = confusionMatrix.getInfo().get(predicted);
      int actIndex = confusionMatrix.getInfo().get(actual);
      confusionMatrix.getMatrix()[actIndex][predIndex]++;
      outKey += actual + "|" + predicted + "|" + percentage + System.lineSeparator();
    }
    outKey = separateBetConfusion(outKey);
    outVal += System.lineSeparator();
    outVal += confusionMatrix.stringPrintedMatrix() + System.lineSeparator();
    outVal = separateBetConfusion(outVal);
    context.write(new Text(outKey), new Text(outVal));
  }

  private String separateBetConfusion(String s) {
    int length = 4;
    for (int i = 0; i < length; i++) {
      s += "#";
    }
    return s;
  }

  private String addLessCharacter(String s, String currWord) {
    int length = 10;
    int currLength = currWord.length();
    while (length - currLength > 0) {
      s += " ";
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
        String currType = splitterBetweenType[splitterBetweenType.length - 1];
        String[] splitterDetail = splitterBetweenType[0].split(",");
        if (currType.trim().equalsIgnoreCase("CLASS")) {
          ClassPriorRed classPriorRed = classContainerRed.getClassPriorRedHashMap()
              .get(splitterDetail[0]);
          if (classPriorRed == null) {
            classPriorRed = new ClassPriorRed(splitterDetail[0]);
            classContainerRed.getClassPriorRedHashMap().put(splitterDetail[0], classPriorRed);
          }
          classPriorRed.getValues().add(splitterDetail[1]);
        }
      }
    }
  }
}
