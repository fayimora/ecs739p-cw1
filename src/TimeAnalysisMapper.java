import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class TimeAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> 
{
    private Text data = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String dump = value.toString();
        int idIdx = StringUtils.ordinalIndexOf(dump, ";", 1) + 1;
        int dateIdx = StringUtils.ordinalIndexOf(dump, ";", 2);
        String dateString = dump.substring(idIdx, dateIdx).split(",")[0].trim();
        
        IntWritable one = new IntWritable(1);
        context.write(new Text(dateString), one);
    }
}
