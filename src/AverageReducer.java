import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<IntWritable, IntIntPair, IntWritable, IntWritable> 
{
    private IntWritable result = new IntWritable();
    
    public void reduce(IntWritable key, Iterable<IntIntPair> values, Context context) throws IOException, InterruptedException 
    {
        int sum = 0;
        int number = 0;
        
        for (IntIntPair value : values) 
        {
          number += value.getFirst().get();
          sum += value.getSecond().get();
        }
        int average = sum/number;
        result.set(average);
        context.write(key, result);
    }
}
