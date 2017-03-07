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

  private static final String HDFS_PATH_OUTPUT_MODEL = "/user/root/bayes/output";
  private static final String HDFS_AUTHORITY = "hdfs://localhost:9000";
  private static final String HDFS_PATH_INFO_INPUT = "/user/root/bayes/info/meta.info";

  /**
   * @param args : [0] input path => data testing [1] output path => result of training
   */
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(conf);
    /* Check if output path (args[1])exist or not */
    if (fs.exists(new Path(args[1]))) {
      /* If exist delete the output path */
      fs.delete(new Path(args[1]), true);
    }

    Path path = new Path(HDFS_AUTHORITY + HDFS_PATH_INFO_INPUT);
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

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
