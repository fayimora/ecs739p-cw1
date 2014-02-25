import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SupportReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    private IntWritable intResult = new IntWritable();
    
    /*This is the reducer. It counts the number of occurances of each key and if it 
        matches the threshold value, it will add the key and result to the context.
    */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
        int threshold = 0;
        for (IntWritable value : values) 
        {
            threshold += value.get();
        }
        
        intResult.set(sum);
            
        //threshold
        if (threshold >= 40)
        {
            context.write(key, intResult);
        }
    }
}
