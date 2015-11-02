package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;

public class MapWorker implements Runnable{

	private MapContext context;
	private Job job;
	private boolean shutdown = false;
	private boolean waiting = false;
	
	public MapWorker(Job job, MapContext context){
		this.context = context;
		this.job = job;
	}
	
	@Override
	public void run() {
		
		while(!shutdown){
			String pair = null;
			synchronized(MapQueue.mapQueue){
				if(!MapQueue.mapQueue.isEmpty()){
					//Retrieve URL from the queue
					pair = MapQueue.mapQueue.remove();
				}else if(MapQueue.mapQueue.isEmpty()){
					//Wait for queue to be not empty
					try {
						waiting = true;
						MapQueue.mapQueue.wait();
					} catch (InterruptedException e) {
						break;
					}
				}
			} // End of synchronized block
			
			waiting = false;
			if(pair!=null){
				String key = pair.split("\t")[0];
				String value = pair.split("\t")[1];
				
				job.map(key,value,context);
			}
		}
		
	}
	
	public boolean isWaiting() {
		return waiting;
	}

	public boolean isShutdown() {
		return shutdown;
	}

	public void setShutdown(boolean shutdown) {
		this.shutdown = shutdown;
	}

}
