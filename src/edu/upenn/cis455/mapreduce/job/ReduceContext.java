package edu.upenn.cis455.mapreduce.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.upenn.cis455.mapreduce.Context;

public class ReduceContext implements Context {

	private File outputfile;
	
	public ReduceContext(File file){
		String filename = file.getAbsolutePath()+"/output.txt";
		this.outputfile = new File(filename);
		if(outputfile.exists())
			outputfile.delete();
		try {
			outputfile.createNewFile();
		} catch (IOException e) {
			System.out.println("could not create output file");
		}
	}
	
	@Override
	public synchronized void write(String key, String value) {
		PrintWriter out;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(outputfile, true)));
			out.println(key+"\t"+value);
			out.close();
		} catch (IOException e) {
			System.out.println("Could not write to output file");
		}
		
		
	}

}
