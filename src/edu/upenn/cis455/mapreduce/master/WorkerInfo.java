package edu.upenn.cis455.mapreduce.master;

import java.util.Date;

public class WorkerInfo {
	
	private String ip;
	private int port;
	private String status;
	private String job;
	private int keysread;
	private int keyswritten;
	private Date last_active;
	
	public WorkerInfo (String ip, int port, String status, String job, int keysread, int keyswritten, Date last_active){
		this.ip = ip;
		this.port = port;
		this.status = status;
		this.job = job;
		this.keysread = keysread;
		this.keyswritten = keyswritten;
		this.last_active = last_active;
	}
	
	public Date getLast_active() {
		return last_active;
	}

	public int getPort() {
		return port;
	}

	public String getStatus() {
		return status;
	}
	
	public void setStatus(String status){
		this.status = status;
	}

	public String getJob() {
		return job;
	}

	public int getKeysread() {
		return keysread;
	}

	public int getKeyswritten() {
		return keyswritten;
	}
	
	public String getIp(){
		return ip;
	}
	

}
