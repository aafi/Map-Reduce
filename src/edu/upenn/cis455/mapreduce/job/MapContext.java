package edu.upenn.cis455.mapreduce.job;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.upenn.cis455.mapreduce.Context;

public class MapContext implements Context{

	String[] worker_list = null;
	String basedir;
	
	public MapContext(String [] worker_list, String basedir){
		this.worker_list = worker_list;
		this.basedir = basedir;
	}
	
	@Override
	public synchronized void write(String key, String value) {
		String hashedKey = hashKey(key);
		int worker_id = decideBucket(hashedKey);
		
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(basedir+worker_id+".txt", true)));
			out.println(key+"\t"+value);
			out.close();
		} catch (IOException e) {
			System.out.println("Could not write to file");
		}
		
	}
	
	/**
	   * This method SHA1 hashes the key and returns the hexadecimal representation
	   * @param key
	   * @return
	   */
	  private String hashKey(String key){
		  MessageDigest md = null;
		  try {
			md = MessageDigest.getInstance("SHA-1");
		  } catch (NoSuchAlgorithmException e) {
			  System.out.println("No such algorithm exception in hashing");
		  }
		  
		  byte [] mdbytes = md.digest(key.getBytes());
		  StringBuffer sb = new StringBuffer();
	      for (int i = 0; i < mdbytes.length; i++) {
	        sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
	      }
	      
		  return sb.toString();
	  }
	  
	  /**
	   * This method decides which bucket the key belongs to
	   * @param hashedKey
	   * @return
	   */
	  
	  private int decideBucket(String hashedKey){
		  String max = "ffffffffffffffffffffffffffffffffffffffff";
		  BigInteger maxInt = new BigInteger(max, 16);
		  BigInteger numWorkers = new BigInteger(Integer.toString(worker_list.length), 16);
		  
		  BigInteger key = new BigInteger(hashedKey,16);
		  BigInteger range = maxInt.divide(numWorkers);
		  
		  return key.divide(range).intValue()+1;
		  
		  
	  }

}
