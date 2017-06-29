package ignite;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;


public class WordCountIgnite extends Configured implements Tool {

	
	
	public static void main(String args[]) throws Exception {
			
					
		int res = ToolRunner.run(new WordCountIgnite(), args);
		System.exit(res);
			
		
	}

	public int run(String[] args) throws Exception {
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "word count");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJarByClass(this.getClass());
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		Ignite ignite;
		IgniteCache<String, Integer> cache;
		IgniteDataStreamer<String, Integer> stmr;
		long count=0;
		
		@Override protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //Ignition.setClientMode(true);
            ignite = Ignition.start("/home/hduser/apache-ignite-2.0.0-src/config/hadoop/default-config2.xml");
    			
    		//CacheConfiguration<String ,Integer> cfg2 = new CacheConfiguration<>();
    		CacheConfiguration<String,Integer> cfg2= Ignition.loadSpringBean("/home/hduser/apache-ignite-2.0.0-src/config/hadoop/default-config2.xml", "cacheconf");
    			
    		
    		cache = ignite.getOrCreateCache(cfg2);
    		cache.put("test", 1993);
    		
    		try{
    			stmr = ignite.dataStreamer("default");
    		}catch(Exception e){
    			
    		}
		 	
        }
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			String line = value.toString();
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			while (tokenizer.hasMoreTokens()) { 
//				word.set(tokenizer.nextToken());
//				context.write(word, one);
//			}
			
			
			String[] lines = tokenize(value.toString());
			
			    
				// Stream entries.
				for(String token : lines){
					if(count % 10000==0){
						
						System.out.println(count+"  "+System.currentTimeMillis()/1000);
						
					}
					//word.set(token);
					//context.write(word, one);
					stmr.addData(token, 1);
					count++;
				}
			
			
			//cache.put("test", 1);
			
		}
		
		@Override protected void cleanup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            ignite.close();
		 	
        }
		
		private String[] tokenize(String text) {
			  text = text.toLowerCase();
			  text = text.replace("'","");
			  text = text.replaceAll("[\\s\\W]+", " ").trim();
			  return text.split(" ");
			}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
		
	}
	

} 

