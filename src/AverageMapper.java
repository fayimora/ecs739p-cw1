import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class AverageMapper extends Mapper<Object, Text, IntWritable, IntIntPair> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String valueString = value.toString();
        
        if(StringUtils.ordinalIndexOf(valueString, ";", 4) > -1) 
        {
          int startIndex = StringUtils.ordinalIndexOf(valueString,";",3) + 1;
          String tweet = valueString.substring(startIndex, valueString.lastIndexOf(';'));
          IntIntPair pair = new IntIntPair(1, tweet.length());
          context.write(one, pair);
        }
    }
}
