package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;

import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {

  static final long serialVersionUID = 455555002;
  
  private String status;
  
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
	  String storagedir = getServletContext().getInitParameter("storagedir");
	  
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
		  String inputdir = request.getParameter("input");
		  int numthreads = Integer.parseInt(request.getParameter("numThreads"));
		  int numworkers = Integer.parseInt(request.getParameter("numWorkers"));
		  
		  String [] workers = new String[numworkers];
		  for(int i = 1;i<numworkers;i++){
			  String name = "worker"+i;
			  workers[i] = request.getParameter(name);
		  }
		  
		  ArrayList<ThreadpoolThread> threadPool = new ArrayList<ThreadpoolThread>();
		  
		  for(int i=0;i<numthreads;i++){
			 MapWorker worker = new MapWorker(workers);
			 ThreadpoolThread thread = new ThreadpoolThread(worker);
			 threadPool.add(thread);
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
  
