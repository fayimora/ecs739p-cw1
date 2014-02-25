import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetLengthCount 
{
    public static void execute(String[] input, String output) throws Exception 
    {
        Configuration conf = new Configuration();
        
        Job hadoopJob = new Job(conf);
        hadoopJob.setJarByClass(TweetLengthCount.class);
        
        hadoopJob.setMapperClass(TweetMapper.class);
        hadoopJob.setReducerClass(IntSumReducer.class);
        
        hadoopJob.setMapOutputKeyClass(Text.class);
        hadoopJob.setMapOutputValueClass(IntWritable.class);
        
        hadoopJob.setNumReduceTasks(4);
        
        Path outputPath = new Path(output);
        
        FileInputFormat.setInputPaths(hadoopJob, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(hadoopJob, outputPath);
        
        outputPath.getFileSystem(conf).delete(outputPath,true);
        hadoopJob.waitForCompletion(true);
    }
    
    public static void main(String[] args) throws Exception 
    {
        execute(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
    }
}
