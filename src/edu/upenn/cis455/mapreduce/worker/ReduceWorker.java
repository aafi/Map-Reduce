package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.ReduceContext;

public class ReduceWorker implements Runnable{

	
	private ReduceContext context;
	private Job job;
	private boolean shutdown = false;
	private boolean waiting = false;

	public ReduceWorker(Job job, ReduceContext context){
		this.context = context;
		this.job = job;
	}
	
	@Override
	public void run() {
		
		while(!shutdown){
			ArrayList<String> list = null;
			synchronized(ReduceQueue.reduceQueue){
				if(!ReduceQueue.reduceQueue.isEmpty()){
					//Retrieve URL from the queue
					list = ReduceQueue.reduceQueue.remove();
				}else if(ReduceQueue.reduceQueue.isEmpty()){
					//Wait for queue to be not empty
					try {
						waiting = true;
						ReduceQueue.reduceQueue.wait();
					} catch (InterruptedException e) {
						break;
					}
				}
			} // End of synchronized block
			
			waiting = false;
			
			if(list!=null){
				String key = list.get(0).split("\t")[0];
				String [] value = new String[list.size()];
				
				for(int i = 0 ;i<list.size();i++){
					String k = list.get(i).split("\t")[0];
					String v = list.get(i).split("\t")[1];
					
					if(!k.equals(key)){
						System.out.println("LIST NOT CORRECT!!");
					}
					
					value[i] = v;
				}
				job.reduce(key, value, context);
			}
		} //End of while
	}// End of run
	
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
