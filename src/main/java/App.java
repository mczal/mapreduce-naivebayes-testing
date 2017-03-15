import java.io.BufferedReader;
import java.io.InputStreamReader;
import mapper.TestingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.TestingReducer;

/**
 * Created by mczal on 22/02/17.
 */
public class App {

  private static final String HDFS_AUTHORITY = "hdfs://localhost:9000";

  /**
   * @param args : [0] model path
   */
  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      String argsExcp =
          "Error catched by custom Impl. Please read the following line below.\n"
              + "-----------------------------\n"
              + "-> Arguments must only consist of 1 path.\n"
              + "-> It located the model of input you want to execute in HDFS.\n"
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
