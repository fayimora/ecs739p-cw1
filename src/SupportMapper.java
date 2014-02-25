import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class SupportMapper extends Mapper<Object, Text, Text, IntWritable> {
  private final IntWritable one = new IntWritable(1);
  private final IntWritable zero = new IntWritable(0);
  private final Text empty = new Text("");
  private final String[] supportPrefixes = new String[] {"team", "go"};
  private Text data = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String dump = value.toString();
    if(StringUtils.ordinalIndexOf(dump, ";", 4) > -1) {
      String supporting = "";
      int startIndex = StringUtils.ordinalIndexOf(dump,";",2) + 1;
      int endIndex = StringUtils.ordinalIndexOf(dump,";",3);
      String[] tags = dump.substring(startIndex, endIndex).split("\\s");
      for (String tag : tags) {
          supporting = getCountry(tag);
          if (!supporting.equals(""))
              break;
      }
      context.write(new Text(supporting), supporting.equals("") ? zero : one);
    }
  }

  public String getCountry(String tag) {
      for (String prefix : supportPrefixes)
          if (tag.startsWith(prefix))
              return tag.substring(prefix.length());
      return "";
  }
}
