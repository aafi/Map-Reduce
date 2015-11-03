package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

/**
 * Implements the map and reduce methods of the job.
 * @author cis455
 *
 */
public class WordCount implements Job {
  
	/**
	 * Map method gets splits the values on spaces and gets the different keys and sends it to context for writing.
	 */
	  public void map(String key, String value, Context context)
	  {
	    // Your map function for WordCount goes here
		for(String word : value.split(" ")){
			context.write(word, "1");
		}
		  
	  }
	  
	  /**
	   * Reduce method counts the number of occurences of each word and sends it to context
	   */
	  public void reduce(String key, String[] values, Context context)
	  {
	    // Your reduce function for WordCount goes here
		  Integer total = 0;
		  for(int i=0;i<values.length;i++){
			  total+=Integer.parseInt(values[i]);
		  }
		  
		  context.write(key, total.toString());
	
	  }
	  
	  
	  
}
