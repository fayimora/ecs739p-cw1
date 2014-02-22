import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<IntWritable, IntIntPair, IntWritable, IntWritable> {
  private IntWritable result = new IntWritable();
  public void reduce(IntWritable key, Iterable<IntIntPair> values, Context context)
    throws IOException, InterruptedException {
    int sum = 0;
    int num = 0;
    for (IntIntPair value : values) {
      num += value.getFirst().get();
      sum += value.getSecond().get();
    }
    int avg = sum/num;
    result.set(avg);
    context.write(key, result);
  }
}
