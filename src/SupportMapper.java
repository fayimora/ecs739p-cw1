import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class SupportMapper extends Mapper<Object, Text, Text, IntWritable> 
{  
    private final IntWritable one = new IntWritable(1);
    private final IntWritable zero = new IntWritable(0);
    private final Text empty = new Text("");
    private final String[] supportPrefixes = new String[] {"team", "go"};
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String dump = value.toString();
        if(StringUtils.ordinalIndexOf(dump, ";", 4) > -1) 
        {
            String supporting = "";
            
            int start = StringUtils.ordinalIndexOf(dump,";",2) + 1;
            int end = StringUtils.ordinalIndexOf(dump,";",3);
            
            String[] tags = dump.substring(start, end).split("\\s");
            
            for (String tag : tags) 
            {
                supporting = getCountry(tag);
                
                //check to see if supporting doesn't equal the empty string
                if (!supporting.equals(""))
                {
                    break;
                }
            }
            
            //write the supporting string into the context as the key and the value as zero is it is the empty string
            //and one if it isn't
            context.write(new Text(supporting), supporting.equals("") ? zero : one);
        }
    }
        
    public String getCountry(String tag) 
    {
        for (String prefix : supportPrefixes)
        {
            if (tag.startsWith(prefix))
            return tag;
        }
        return tag.substring(prefix.length());
        return "";
    }
}