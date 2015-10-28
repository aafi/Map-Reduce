package edu.upenn.cis455.mapreduce.master;

public class JobInfo {
	
	private String job;
	private String input;
	private String num_map_threads;
	private String output;
	private String num_reduce_threads;
	private int num_active;
	
	public JobInfo(String job, String input, String num_map_threads, int num_active, String output, String num_reduce_threads){
		this.job = job;
		this.input = input;
		this.num_map_threads = num_map_threads;
		this.num_active = num_active;
		this.output = output;
		this.num_reduce_threads = num_reduce_threads;
	}
	
	public String getOutput() {
		return output;
	}

	public String getNum_reduce_threads() {
		return num_reduce_threads;
	}
	
	public int getNum_active() {
		return num_active;
	}
	
	public String getJob() {
		return job;
	}

	public String getInput() {
		return input;
	}

	public String getNum_map_threads() {
		return num_map_threads;
	}
	
}
