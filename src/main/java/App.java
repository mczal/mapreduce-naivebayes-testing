import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import mapper.TestingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.TestingReducer;

/**
 * Created by mczal on 22/02/17.
 */
public class App {

  private static String HDFS_AUTHORITY;

  private static void loadProperties(String fileLoc) throws Exception {
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileLoc));
      Properties props = new Properties();
      props.load(br);
      HDFS_AUTHORITY = props.getProperty("hdfs.authority");
    } catch (FileNotFoundException fnfe) {
      throw new FileNotFoundException(
          "Error catched by custom Impl. Please read the following line below.\n"
              + "-----------------------------\n"
              + "-> Your .properties file not found.");
    } catch (IOException e) {
      throw new IOException(
          "Error catched by custom Impl. Please read the following line below.\n"
              + "-----------------------------\n"
              + "-> IO Exception OCCURED.");
    }
  }

  /**
   * @param args : [0] model path
   */
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      String argsExcp =
          "Error catched by custom Impl. Please read the following line below.\n"
              + "-----------------------------\n"
              + "-> Arguments must only consist of 2 path.\n"
              + "-> It located the model of input you want to execute in HDFS .properties file for specify HDFS url.\n"
              + "-> Ex: \n"
              + "-> If args[0]=/user/root/bayes/weather -> \n"
              + "-> Then, that path must had : \n"
              + "-> (1) info path + file => /user/root/bayes/weather/info/meta.info\n"
              + "-> (2) model output path + file => /user/root/bayes/weather/output/...\n"
              + "-> (3) testing path for input split file => /user/root/bayes/weather/testing/input/...\n"
              + "-> The output file will be located in /user/root/bayes/weather/output/...\n"
              + "-----------------------------";
      throw new IllegalArgumentException(argsExcp);
    }

    if (!args[1].contains(".properties")) {
      throw new IllegalArgumentException(
          "Error catched by custom Impl. Please read the following line below.\n"
              + "-----------------------------\n"
              + "-> Second arguments must specifying your path for .properties file.");
    }
    loadProperties(args[1]);

    Configuration conf = new Configuration();

    String infoPathFile = args[0];
    String inputPath = args[0];
    String outputPath = args[0];
    String outputModelPath = args[0]; // For model classifier that has been generated with different module program
    if (args[0].charAt(args[0].length() - 1) == '/') {
      inputPath += "testing/input";
      outputPath += "testing/output";
      infoPathFile += "info/meta.info";
      outputModelPath += "output"; // For model classifier that has been generated with different module program
    } else {
      inputPath += "/testing/input";
      outputPath += "/testing/output";
      infoPathFile += "/info/meta.info";
      outputModelPath += "/output"; // For model classifier that has been generated with different module program
    }

    conf.set("outputModelPath", outputModelPath);
    conf.set("laplacianSmoothingAdder", "1");
    conf.set("hdfs.authority", HDFS_AUTHORITY);

    FileSystem fs = FileSystem.get(conf);
    /* Check if output path (outputPath)exist or not */
    if (fs.exists(new Path(outputPath))) {
      /* If exist delete the output path */
      fs.delete(new Path(outputPath), true);
    }
    Path path = new Path(HDFS_AUTHORITY + infoPathFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    String currClass = br.readLine();
    String currAttr = br.readLine();

    conf.set("classes", currClass.split(":")[1]);
    conf.set("attributes", currAttr.split(":")[1]);

    Job job = Job.getInstance(conf, "bayes_testing");
    job.setJarByClass(App.class);
    job.setMapperClass(TestingMapper.class);
//    job.setCombinerClass(MyReducer.classp);
    job.setReducerClass(TestingReducer.class);
    job.setMapOutputKeyClass(Text.class); // your mapper - not shown in this example
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
