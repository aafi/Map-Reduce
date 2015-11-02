package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  
  HashMap<String,WorkerInfo> workermappings = new HashMap<String,WorkerInfo>();
  Queue <JobInfo> job_queue = new LinkedList <JobInfo>();
  ArrayList <WorkerInfo> current_workers = new ArrayList <WorkerInfo>();
  JobInfo current_job;

  public void doPost(HttpServletRequest request, HttpServletResponse response) 
	       throws java.io.IOException{
	  
	  Date current = new Date();
	  String job =  request.getParameter("job");
	  String input = request.getParameter("inputdir");
	  String num_map_threads = request.getParameter("mapthreads");
	  String output = request.getParameter("output");
	  String num_reduce_threads = request.getParameter("reducethreads");
	  int num_active = 0;
	  
	  Iterator<Entry<String, WorkerInfo>> iterator = workermappings.entrySet().iterator();
      while (iterator.hasNext()) {
          Map.Entry pair = (Map.Entry)iterator.next();
          WorkerInfo worker = (WorkerInfo) pair.getValue();
          Date last_active = worker.getLast_active();
          if (current.getTime() - last_active.getTime() < 30*1000) {
        	  num_active++;
          }
          
      }
      
	  JobInfo jobinfo = new JobInfo(job, input, num_map_threads,num_active,output, num_reduce_threads);
	  job_queue.add(jobinfo);
  }
  
  //GET workerstatus and GET status
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
    response.setContentType("text/html");
    String pathinfo = request.getPathInfo().substring(1);
    
    //Store the information sent by a worker
    if(pathinfo.startsWith("workerstatus")){
    	Date current = new Date();
	    int port = Integer.parseInt(request.getParameter("port"));
	    String status = request.getParameter("status");
	    String job = request.getParameter("job");
	    int keysread = Integer.parseInt(request.getParameter("keysread"));
	    int keyswritten = Integer.parseInt(request.getParameter("keyswritten"));
	    
	    String ip = request.getRemoteAddr();
	    WorkerInfo info = new WorkerInfo(ip,port,status,job,keysread,keyswritten, new Date());
	    String key = ip+":"+port;
	    workermappings.put(key, info);
	    
	    /**
	     * Check if all the current workers are waiting, then send reduce job
	     */
	    boolean all_waiting = true;
	    
	    for(WorkerInfo worker : this.current_workers){
	    	if(!worker.getStatus().equals("waiting")){
	    		all_waiting = false;
	    		break;
	    	}
	    }
	    
	    if(all_waiting){
	    	for(WorkerInfo worker : this.current_workers){
	    		String ip_addr = worker.getIp();
                int port_num = worker.getPort();
                Socket socket = new Socket(ip_addr,port_num);
                
                String requestLine = "POST /runreduce HTTP/1.0 \r\n"
						  +"\r\n";
                
                String body = "job="+current_job.getJob()+"&output="+current_job.getOutput()+"&numThreads="+current_job.getNum_reduce_threads()
                		+"\r\n";
                String message = requestLine+body;
                
                OutputStream output = socket.getOutputStream();
        		output.write(message.getBytes());
        		output.close();
        		socket.close();
        		
		    }
	    }
	    
	    
	    /**
	     * Check if all the workers are idle. If yes then send the next job in the queue
	     */
	    Iterator<Entry<String, WorkerInfo>> it = workermappings.entrySet().iterator();
	    boolean all_active = true;
	    int num_active = 0;
	    
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            WorkerInfo worker = (WorkerInfo) pair.getValue();
            Date last_active = worker.getLast_active();
            if (current.getTime() - last_active.getTime() < 30*1000) {
            	if(!worker.getStatus().equals("idle")){
            		all_active = false;
            		break;
            	}else{
            		num_active++;
            	}
            }else{
            	System.out.println("Removing inactives");
            	it.remove();
            }
        }
        
        //if all the threads are idle, send next job
        if(all_active && !job_queue.isEmpty()){
        	JobInfo jobinfo = job_queue.remove();
        	this.current_job = jobinfo;
        	Iterator<Entry<String, WorkerInfo>> iterator = workermappings.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry pair = (Map.Entry)iterator.next();
                WorkerInfo worker = (WorkerInfo) pair.getValue();
                
                String ip_addr = worker.getIp();
                int port_num = worker.getPort();
                
                Socket socket = new Socket(ip_addr,port_num);
                
                String body = "job="+jobinfo.getJob()+"&input="+jobinfo.getInput()+"&numThreads="+jobinfo.getNum_map_threads()
                		+"&numWorkers="+num_active;
                
                int workernum = 1;
                for(String mapkey : workermappings.keySet()){
                	body = body+"&worker"+workernum+"="+mapkey;
                	workernum++;
                }
                
                body = body+"\r\n";
                
                String requestLine = "POST /worker/runmap HTTP/1.0\r\n"
                		  +"Content-Type: application/x-www-form-urlencoded\r\n"
                		  +"Content-Length: "+body.getBytes().length+"\r\n"
						  +"\r\n";
              
                String message = requestLine+body;
                
                
                OutputStream output = socket.getOutputStream();
        		output.write(message.getBytes());
        		output.flush();
        		output.close();
        		socket.close();
        		System.out.println("sent job");
        		current_workers.add(worker);
            }
        }
	    
	    
	    
    }else if(pathinfo.startsWith("status")){ //Display worker information and job form
    	Date current = new Date();
    	PrintWriter out = response.getWriter();
    	StringBuilder sb = new StringBuilder();
    	
    	
    	sb.append("The following workers are currently active: <br><br><br>");
    	sb.append("<table style=\"width:100%\" align=\"left\" border=\"1\" \">");
    	sb.append("<tr>"+
    	    "<th>Worker</th>"+
    	    "<th>ip:port</th>"+
    	    "<th>status</th>"+
    	    "<th>job</th>"+
    	    "<th>keys read</th>"+
    	    "<th>keys written</th>"+
    		"</tr>");
    	
    	int idx =1;
    	Iterator<Entry<String, WorkerInfo>> it = workermappings.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            WorkerInfo info = (WorkerInfo) pair.getValue();
            Date last_active = info.getLast_active();
            if (current.getTime() - last_active.getTime() < 30*1000) {
	            sb.append("<tr>");
	            sb.append("<td>"+idx+++"</td>");
	            sb.append("<td>"+info.getIp()+":"+info.getPort()+"</td>");
	            sb.append("<td>"+info.getStatus()+"</td>");
	            sb.append("<td>"+info.getJob()+"</td>");
	            sb.append("<td>"+info.getKeysread()+"</td>");
	            sb.append("<td>"+info.getKeyswritten()+"</td>");
            }else{
            	//it.remove();
            }
        }
        
        sb.append("</table>");
        
        String header = "<br><br><br>Job Input Form <br><br>";
		
		String body = header
				  +"<form action=\"/master/\" method = \"post\">"
				  +"Class Name of Job: <br>"
				  +"<input type=\"text\" name=\"job\" <br> <br>"
				  +"Input Directory: <br>"
				  +"<input type=\"text\" name=\"inputdir\" <br> <br>"
				  +"Output Directory: <br>"
				  +"<input type=\"text\" name=\"outputdir\" <br> <br>"
				  +"Number of threads for map: <br>"
				  +"<input type=\"text\" name=\"mapthreads\" <br> <br>"
				  +"Number of threads for reduce: <br>"
				  +"<input type=\"text\" name=\"reducethreads\" <br> <br>"
				  +"<input type=\"submit\" value=\"Submit\">";
		
		sb.append(body);
		
        String page = Utilities.createHTML("Status", sb.toString());
        out.write(page);
    }
    

  }
}
  
