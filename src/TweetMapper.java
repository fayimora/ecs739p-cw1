import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> 
{
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String dataDump = value.toString();
        if(StringUtils.ordinalIndexOf(dataDump, ";", 4) > -1) 
        {
            int startIndex = StringUtils.ordinalIndexOf(dataDump,";",3) + 1;
            
            String tweet = dataDump.substring(startIndex, dataDump.lastIndexOf(';'));
            data.set(""+tweet.length());
            
            context.write(data, one);
        }
    }
}
