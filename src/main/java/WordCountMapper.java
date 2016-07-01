import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author nik
 * @since 1.0.0-SNAPSHOT
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
    final StringTokenizer st = new StringTokenizer(value.toString());
    final String regExp = "[^\\w\\d]"; // kill all non-aphanumerics
    while (st.hasMoreElements()) {
      final String element;
      if ((element = st.nextElement().toString().replaceAll(regExp, "").toLowerCase()).length() > 2) {
        /**
         * Solution 1
         * /
         context.write(new Text(context.getInputSplit() + "\t\t" + element + "\t"), new IntWritable(1));
         /**
         *
         */

        /**
         * Solution 2
         * /
         context.write(new Text(context.getInputSplit() + "\t\t" + element + "\t"), new IntWritable(1));
         /**
         *
         */

        /**
         * Solution 3
         */
        context.write(new Text(element + "\t"), new Text(1 + "::" + ((FileSplit) context.getInputSplit()).getPath().getName()));
      }
    }
  }

}
