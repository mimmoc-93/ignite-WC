package ignite;
import java.io.IOException;
import java.util.StringTokenizer;
    
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    
public class WordCount {
    
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    // Making objects is expensive. Instantiate outside the loop and re-use
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      // Whilst iterating over the token iterator
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());  // Store the next token in our Text object
        context.write(word, one);  // Give a <word, 1> pair
      }
    }
  }
    
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      result.set(sum);
      context.write(key, result);
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    
    // Make this class the main in the JAR file
    job.setJarByClass(WordCount.class);
    
    // Set out Mapper class, conforming to the API
    job.setMapperClass(TokenizerMapper.class);
    
    // Set out Combiner & Reducer classes, conforming to the (same) API
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    // Set the ouput Key type
    job.setOutputKeyClass(Text.class);
    
    // Set the output Value type
    job.setOutputValueClass(IntWritable.class);
    
    // Set number of reducers
    job.setNumReduceTasks(10);
   
    // Get the input and output paths from the job arguments
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}