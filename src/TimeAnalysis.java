import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TimeAnalysis 
{
    public static void execute(String[] input, String output) throws Exception 
    {
        Configuration conf = new Configuration();
        
        Job haoopJob = new Job(conf);
        haoopJob.setJarByClass(TweetLengthCount.class);
        
        haoopJob.setMapperClass(TimeAnalysisMapper.class);
        haoopJob.setReducerClass(TimeAnalysisReducer.class);
        
        haoopJob.setMapOutputKeyClass(Text.class);
        haoopJob.setMapOutputValueClass(IntWritable.class);
        
        haoopJob.setNumReduceTasks(4);
        
        Path outputPath = new Path(output);
        
        FileInputFormat.setInputPaths(haoopJob, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(haoopJob, outputPath);
        
        outputPath.getFileSystem(conf).delete(outputPath,true);
        haoopJob.waitForCompletion(true);
    }
    
    public static void main(String[] args) throws Exception 
    {
        execute(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
    }
}