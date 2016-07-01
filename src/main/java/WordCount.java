import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author nik
 * @since 1.0.0-SNAPSHOT
 */
public class WordCount {

  public static void main(String... args) {
    // create and initialize job Object
    try {
      final Job job = Job.getInstance();
      job.setJarByClass(WordCount.class);
      job.setJobName("WordCount db07");

      // specify input and output directories
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // specify mapper
      job.setMapperClass(WordCountMapper.class);

      // specify a reducer and a combiner
      job.setReducerClass(WordCountReducer.class);
      job.setCombinerClass(WordCountReducer.class);

      // specify output types
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // start job and wait for completion
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

}
