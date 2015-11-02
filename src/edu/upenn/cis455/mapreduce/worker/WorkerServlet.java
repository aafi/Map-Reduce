package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;
import edu.upenn.cis455.mapreduce.job.ReduceContext;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  private String status;
  private String storagedir;
  private HashMap <File,String> fileMappings = new HashMap <File,String>();
  private boolean shutdown = false;
  
  private String master_ip;
  private int master_port;
  private String worker_port;
  private String job;
  
  private Integer keysread;
  private Integer keyswritten;
  
  private MapContext mapcontext;
  private ReduceContext reducecontext;
  
  public String getStatus() {
	return status;
  }

  /**
   * Spawn new heart beat thread
   */
  public void init(ServletConfig config){
	  
	  status = "idle";
	  //Get master parameter
	  String master = config.getInitParameter("master");
	  storagedir = config.getInitParameter("storagedir");
	  
	  this.master_ip = master.split(":")[0];
	  this.master_port = Integer.parseInt(master.split(":")[1]);
	  
	  worker_port = config.getInitParameter("port");
	  keysread = 0;
	  keyswritten = 0;
	  
	  HeartBeat beat = new HeartBeat();
	  Thread t = new Thread(beat);
	  t.start();
  }
  
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String pathinfo = request.getPathInfo().substring(1);
	  
	  // run the mapping threads
	  if(pathinfo.equals("runmap")){
		  
		  job = request.getParameter("job");
		  File inputdir = new File(storagedir+request.getParameter("input"));
		  
		  int numthreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numworkers = Integer.parseInt(request.getParameter("numWorkers"));
		  
		  String [] workers = new String[numworkers];
		  for(int i = 1;i<=numworkers;i++){
			  String name = "worker"+i;
			  workers[i-1] = request.getParameter(name);
		  }
		  
		  /**
		   * LOAD JOB CLASS
		   */
		  
		  Class<Job> job_class = null;
		  
		  try {
			job_class = (Class<Job>) Class.forName(job);
		  } catch (ClassNotFoundException e) {
				System.out.println("Job class not found");
		  }
		  
		  Job jobclass = null;
		  
		  try {
			jobclass = job_class.newInstance();
		  } catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  
		  /**
		   * Check for spool-out and spool-in directories
		   */
		  File spool_in = new File(storagedir+"spool_in");
		  File spool_out = new File(storagedir+"spool_out");
		  
		  checkDir(spool_in);
		  checkDir(spool_out);
		  
		  makeSpoolOutFiles(storagedir+"spool_out",workers.length,workers);
		  
		  mapcontext = new MapContext(workers,storagedir+"spool_out/");
		  
		  synchronized(keysread){
		  	keysread = 0;
		  }
		  
		  ArrayList<ThreadpoolThread> threadPool = new ArrayList<ThreadpoolThread>();
		  
		  for(int i=0;i<numthreads;i++){
			 MapWorker worker = new MapWorker(jobclass,mapcontext);
			 ThreadpoolThread thread = new ThreadpoolThread(worker);
			 threadPool.add(thread);
		  }
		  
		  /**
		   * Read all key value pairs from the files in the input directory and add to queue
		   */
		  for(File file : inputdir.listFiles()){
			  status = "mapping";
			  BufferedReader br = new BufferedReader(new FileReader(file));
			  String currentLine = null;
			  
			  while((currentLine = br.readLine())!=null){
				  synchronized(keysread){
					  keysread++;
				  }
				  
				  synchronized(MapQueue.mapQueue){
					  MapQueue.mapQueue.add(currentLine);
					  MapQueue.mapQueue.notifyAll();
				  }
			  }
			  br.close();
		  }
		  
		  shutdown = false;
			
		  while(!shutdown){
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					System.out.println("Main thread interrupted");
				}
				
				shutdown = true;
				
				synchronized(MapQueue.mapQueue){
					if(!MapQueue.mapQueue.isEmpty()){
						shutdown = false;
					}
					
				}
				
				for(ThreadpoolThread t : threadPool){
					if(!t.getMapWorker().isWaiting()){
						shutdown = false;
					}
				}
				
				if(shutdown){
					for(ThreadpoolThread t : threadPool){
						t.getMapWorker().setShutdown(true);
					}
					
					synchronized(MapQueue.mapQueue){
						MapQueue.mapQueue.notifyAll();
					}
					
				}
		  } // End of while
		  
		  for(ThreadpoolThread t : threadPool){
				try {
					t.getThread().join();
				} catch (InterruptedException e) {
					System.out.println("Threads could not join");
				}
			}
		  
		  /**
		   * All threads have finished executing.
		   * Send push data signal
		   */
		  Iterator<Entry<File, String>> it = fileMappings.entrySet().iterator();
		  while(it.hasNext()){
			  Map.Entry pair = (Map.Entry)it.next();
			  String portstring = ((String) pair.getValue()).split(":")[1];
			  String ip = ((String) pair.getValue()).split(":")[0];
			  int port = Integer.parseInt(portstring.trim());
			  
			  File file = (File) pair.getKey();
			  BufferedReader br = new BufferedReader(new FileReader(file));
			  StringBuilder body = new StringBuilder();
			  int b;
			  while((b = br.read())!=-1){
				  body.append((char)b);
			  }
			  
			  br.close();
			  Socket socket = new Socket(ip,port);
			  String request_line = "POST /worker/pushdata HTTP/1.0\r\n"
					  				+"Content-Length: "+body.toString().getBytes().length+"\r\n"
					  				+"Content-Type: text/plain \r\n\r\n";
			  
			  String message = request_line+body.toString();
			  
			  socket.getOutputStream().write(message.getBytes());
			  socket.close();
		  }
		  
		  status = "waiting";
		  sendWorkerStatus();
		  
	  }
	  
	  //run the reducer threads
	  else if(pathinfo.equals("runreduce")){
		  System.out.println("received run reduce");
		  
		  File outputdir = new File(storagedir+request.getParameter("output"));
		  checkDir(outputdir);
		  
		  reducecontext = new ReduceContext(outputdir);
		  reducecontext.keyswritten = 0;
		  
		  status = "reducing";
		  sendWorkerStatus();
		  
		  //sort spool-in
		  File file = new File(storagedir+"spool_in/output.txt");
		  
		  job = request.getParameter("job");
		  
		  
		  int numthreads = Integer.parseInt(request.getParameter("numThreads"));
		  
		  /**
		   * LOAD JOB CLASS
		   */
		  
		  Class<Job> job_class = null;
		  
		  try {
			job_class = (Class<Job>) Class.forName(job);
		  } catch (ClassNotFoundException e) {
				System.out.println("Job class not found");
		  }
		  
		  Job jobclass = null;
		  
		  try {
			jobclass = job_class.newInstance();
		  } catch (InstantiationException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  
		  synchronized(keysread){
			  	keysread = 0;
		  }
		  
		  ArrayList<ThreadpoolThread> threadPool = new ArrayList<ThreadpoolThread>();
		  
		  for(int i=0;i<numthreads;i++){
			 ReduceWorker worker = new ReduceWorker(jobclass,reducecontext);
			 ThreadpoolThread thread = new ThreadpoolThread(worker);
			 threadPool.add(thread);
		  }
		  
		  sortFile(file);
		  int n;
		  synchronized(keysread){
			 n = keysread;
		  }
		  System.out.println("Done at "+new Date().toString()+" keysread: "+n);
		  shutdown = false;
			
		  while(!shutdown){
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					System.out.println("Main thread interrupted");
				}
				
				shutdown = true;
					
				synchronized(ReduceQueue.reduceQueue){
					if(!ReduceQueue.reduceQueue.isEmpty()){
						shutdown = false;
					}
				}
				
				for(ThreadpoolThread t : threadPool){
					if(!t.getReduceWorker().isWaiting()){
						shutdown = false;
					}
				}
				
				
				
				if(shutdown){
					System.out.println("shutting down at "+new Date().toString());
					for(ThreadpoolThread t : threadPool){
						t.getReduceWorker().setShutdown(true);
					}
					
					synchronized(ReduceQueue.reduceQueue){
						ReduceQueue.reduceQueue.notifyAll();
					}
					
				}
		  } // End of while
		  
		  int num = 0;
		  for(ThreadpoolThread t : threadPool){
				try {
					t.getThread().join();
					num++;
				} catch (InterruptedException e) {
					System.out.println("Reduce Threads could not join");
				}
		  	}
		  
		  System.out.println("All thread joined "+num);
		  status = "idle";
		  sendWorkerStatus();
	  }
	  
	  //write data out to file
	  else if(pathinfo.equals("pushdata")){
		  Integer length = Integer.parseInt(request.getHeader("Content-Length"));
		  
		  BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream()));
		  String body =null;
		  if(length!=null){
				int total_read = 0;
				int b;
				StringBuilder s = new StringBuilder();
				while(total_read<length && ((b = br.read())!=-1)){
					s.append((char)b);
					total_read++;
				}
				body = s.toString();
		  }
		  
		  File file = new File(storagedir+"spool_in/output.txt");
		  if(!file.exists()){
			  file.createNewFile();
//			  System.out.println("created file "+file.getPath());
		  }
		  
		  synchronized(file){
			  BufferedWriter out = new BufferedWriter(new FileWriter(file, true));
			  out.write(body);
			  out.close();
		  }
	  } // End of pushdata
  }
  
  /**
   * Sorts a given file by key for reduce phase.
   * Populates the reduce queue.
   * @param file
   */
  private boolean sortFile(File file) {
	String command = "sort "+file.getAbsolutePath();
	try {
		Process p = Runtime.getRuntime().exec(command);
		BufferedReader in = new BufferedReader(
	               new InputStreamReader(p.getInputStream()) );
		
		String line;
		String prev_word = null;
		ArrayList<String> list = null;
		
	    while ((line = in.readLine()) != null) {
	    	synchronized(keysread){ 
	    		keysread++;
	    	}
	    	
	         if(line.split("\t")[0].equals(prev_word)){
	        	 list.add(line);
	        	 prev_word = line.split("\t")[0];
	         }else{
	        	 if(list!=null){
		        	 synchronized(ReduceQueue.reduceQueue){
		        		 ReduceQueue.reduceQueue.add(list);
		        		 ReduceQueue.reduceQueue.notifyAll();
		        	 }
	        	 }
	        	 
	        	 list = new ArrayList<String>();
	        	 boolean added = list.add(line);
	        	 
	        	 System.out.println("Added new list for "+line.split("\t")[0]+" "+added);
	        	 prev_word = line.split("\t")[0];
	         }
	    }
	    in.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	return true;
  }

/**
   * Checks if the given dir exists. 
   * If it does, deletes the directory and makes it again.
   * @param dir
   */
  private void checkDir(File dir) {
	  System.out.println(dir.getAbsolutePath());
	  if(dir.exists()){
		  for(File file : dir.listFiles()){
			 file.delete();
		  }
		  
		  dir.delete();
	  }
	  
	  dir.mkdir();
	  System.out.println(dir.getAbsolutePath()+" created");
  }
  
  /**
   * Creates a file for each worker in the spool out directory
   * @param basedir
   * @param num
   * @param workers
   * @throws IOException
   */
  
  private void makeSpoolOutFiles(String basedir,int num, String [] workers) throws IOException{
	  for(int i=0;i<workers.length;i++){
		  File file = new File(basedir+"/"+(i+1)+".txt");
		  boolean result = file.createNewFile();
		  fileMappings.put(file, workers[i]);
	  }
	  
  }
  
  /**
	 * Sends the worker status to the master servlet
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public void sendWorkerStatus(){
		Socket socket = null;
		boolean connected = false;
		
		while(!connected){
			try {
				socket = new Socket(master_ip,master_port);
				connected = true;
			} catch (IOException e1) {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
				}
				System.out.println("Server not started. Trying again");
			}
		}
		
		StringBuilder url = new StringBuilder();
		
		url.append("/master/workerstatus?");
		url.append("port="+worker_port);
		url.append("&status="+status);
		url.append("&job="+job);
		
		url.append("&keysread="+getkeysread());
		url.append("&keyswritten="+getkeyswritten());
		String message = "GET "+url.toString()+" HTTP/1.0\r\n\r\n";
		
		try {
			socket.getOutputStream().write(message.getBytes());
			socket.close();
		} catch (IOException e) {
			System.out.println("Could not send status");
		}
		
	}
  
  private int getkeysread() {
	if(status.equals("idle"))
		return 0;
	
	int n = 0;
	
	synchronized(keysread){
		n = keysread;
	}
	return n;
  }

  private int getkeyswritten() {
	  if(mapcontext == null)
		  return 0;
	  
	  if(status.equals("mapping") || status.equals("waiting"))
		  return mapcontext.keyswritten;
	  else if(status.equals("reducing") || status.equals("idle"))
		  return reducecontext.keyswritten;
	  
	  return 0;
	 
  }

/**
   * Class to send heart beat
   * @author cis455
   *
   */
  
  public class HeartBeat implements Runnable{

		@Override
		public void run() {
			while(true){
				sendWorkerStatus();
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					System.out.println("Heart Beat thread interrupted");
				}
				
			}
		}
		
	} //End of heartbeat class
}
  
