package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;

public class MapWorker implements Runnable{

	private MapContext context;
	private Job job;
	
	public MapWorker(Job job, MapContext context){
		this.context = context;
		this.job = job;
	}
	
	@Override
	public void run() {
		
		while(true){
			String pair = null;
			synchronized(MapQueue.mapQueue){
				if(!MapQueue.mapQueue.isEmpty()){
					//Retrieve URL from the queue
					pair = MapQueue.mapQueue.remove();
				}else if(MapQueue.mapQueue.isEmpty()){
					//Wait for queue to be not empty
					try {
						MapQueue.mapQueue.wait();
					} catch (InterruptedException e) {
						break;
					}
				}
			} // End of synchronized block
			
			if(pair!=null){
				String key = pair.split("\t")[0];
				String value = pair.split("\t")[1];
				
				job.map(key,value,context);
			}
		}
		
	}

}
