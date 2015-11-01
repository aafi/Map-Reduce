package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.MapContext;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  private String status;
  private String storagedir;
  
  public String getStatus() {
	return status;
}

  /**
   * Spawn new heart beat thread
   */
  public void init(ServletConfig config){
	  status = "idle";
	  //Get master parameter
	  String master = getServletContext().getInitParameter("master");
	  storagedir = getServletContext().getInitParameter("storagedir");
	  
	  HeartBeat beat = new HeartBeat(master);
	  Thread t = new Thread(beat);
	  t.start();
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  String pathinfo = request.getPathInfo().substring(1);
	  
	  // run the mapping threads
	  if(pathinfo.equals("runmap")){
		  String job = request.getParameter("job");
		  File inputdir = new File(storagedir+request.getParameter("input"));
		  
		  int numthreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numworkers = Integer.parseInt(request.getParameter("numWorkers"));
		  
		  String [] workers = new String[numworkers];
		  for(int i = 1;i<numworkers;i++){
			  String name = "worker"+i;
			  workers[i] = request.getParameter(name);
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
		  
		  MapContext context = new MapContext(workers);
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
			  BufferedReader br = new BufferedReader(new FileReader(file));
			  String currentLine = null;
			  
			  while((currentLine = br.readLine())!=null){
				  MapQueue.mapQueue.add(currentLine);
				  MapQueue.mapQueue.notifyAll();
			  }
			  br.close();
		  }
		  
		  
	  }
	  
	  //run the reducer threads
	  else if(pathinfo.equals("runreduce")){
		  
	  }
	  
	  //write data out to file
	  else if(pathinfo.equals("pushdata")){
		  
	  }
    
  }
}
  
