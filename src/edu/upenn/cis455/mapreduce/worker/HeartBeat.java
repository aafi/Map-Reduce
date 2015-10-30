package edu.upenn.cis455.mapreduce.worker;

public class HeartBeat implements Runnable{

	String master_ip;
	int master_port;
	
	public HeartBeat(String masterinfo){
		master_ip = masterinfo.split(":")[0];
		master_port = Integer.parseInt(masterinfo.split(":")[1]);
	}
	
	@Override
	public void run() {
		
		
	}
	
	/**
	 * Sends the worker status to the master servlet
	 */
	public static void sendWorkerStatus(){
		
	}

}
