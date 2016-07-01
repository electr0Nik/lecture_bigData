import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author nik
 * @since 1.0.0-SNAPSHOT
 */
public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
    /**
     * Solution 1 and 2
     * /
     int[] result = {0};
     values.forEach(it -> result[0] += Integer.parseInt(it.toString()));
     context.write(key, new IntWritable(result[0]));
     /**
     *
     */

    /**
     * Solution 3
     */
    String result = "";
    for (final Text element : values) {
      if (!result.contains(element.toString())) {
        result += element;
      }
    }
    context.write(key, new Text(result));

  }

}
