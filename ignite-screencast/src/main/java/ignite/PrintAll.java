package ignite;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.cache.Cache.Entry;

import org.apache.hadoop.util.ToolRunner;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;


public class PrintAll{

	public static void main(String args[]) throws Exception {
		Ignition.setClientMode(true);
		try(Ignite ignite = Ignition.start("/home/hduser/apache-ignite-2.0.0-src/examples/config/example-cache1.xml")){
			
			CacheConfiguration<String,Integer> cfg2= Ignition.loadSpringBean("/home/hduser/apache-ignite-2.0.0-src/examples/config/example-cache1.xml", "cacheconf"); 
			
			
			IgniteCache<String, Integer> cache;
			
			cache = ignite.getOrCreateCache(cfg2);
			
				
	        Iterator<Entry<String,Integer>> it = cache.iterator();
	        int i =0;
	        while(it.hasNext()){
	        	Entry<String,Integer> temp = it.next();
	        	System.out.println(temp.getKey()+"  ->  "+temp.getValue());
	        	i++;
	        }
	        System.out.println("Lunghezza = "+i);
			
		}	
	}	
} 
	