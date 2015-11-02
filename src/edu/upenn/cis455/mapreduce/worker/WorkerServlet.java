package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;

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
		  
		  MapContext context = new MapContext(workers,storagedir+"spool_out/");
		  ArrayList<ThreadpoolThread> threadPool = new ArrayList<ThreadpoolThread>();
		  
		  for(int i=0;i<numthreads;i++){
			 MapWorker worker = new MapWorker(jobclass,context);
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
					
					for(ThreadpoolThread t : threadPool){
						if(!t.getWorker().isWaiting()){
							shutdown = false;
						}
					}
				}
				
				if(shutdown){
					for(ThreadpoolThread t : threadPool){
						t.getWorker().setShutdown(true);
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
	  }
	  
	  //write data out to file
	  else if(pathinfo.equals("pushdata")){
		  System.out.println("received pushdata");
		  
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
			  System.out.println("created file "+file.getPath());
		  }
		  
		  synchronized(file){
			  BufferedWriter out = new BufferedWriter(new FileWriter(file, true));
			  out.write(body);
			  out.close();
		  }
		  
	  }
    
  }
  
  /**
   * Checks if the given dir exists. 
   * If it does, deletes the directory and makes it again.
   * @param dir
   */
  private void checkDir(File dir) {
	  if(dir.exists()){
		  for(File file : dir.listFiles()){
			 boolean result = file.delete();
		  }
		  
		  dir.delete();
	  }
	  
	  dir.mkdir();
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
		
		//TODO
		url.append("&keysread=0");
		url.append("&keyswritten=0");
		String message = "GET "+url.toString()+" HTTP/1.0\r\n\r\n";
		
		try {
			socket.getOutputStream().write(message.getBytes());
			socket.close();
		} catch (IOException e) {
			System.out.println("Could not send status");
		}
		
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
  
