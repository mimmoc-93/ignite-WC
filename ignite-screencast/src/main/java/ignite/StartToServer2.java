package ignite;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.stream.StreamTransformer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;


public class StartToServer2{

	public static void main(String args[]) throws Exception {
		Ignition.setClientMode(false);
		
		try(Ignite ignite = Ignition.start("/home/hduser/apache-ignite-2.0.0-src/examples/config/example-cache1.xml")){
			
			CacheConfiguration<String,Long> cfg2= Ignition.loadSpringBean("/home/hduser/apache-ignite-2.0.0-src/examples/config/example-cache1.xml", "cacheconf"); 
			
			
			IgniteCache<String, Long> cache;
			cache = ignite.getOrCreateCache(cfg2);
			
			
			while(true){
				
			}
		}	
		
		
		
		
	}
	


} 