package edu.upenn.cis455.mapreduce.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.upenn.cis455.mapreduce.Context;

public class ReduceContext implements Context {

	private File outputfile;
	public int keyswritten = 0;
	
	public ReduceContext(File file){
		String filename = file.getAbsolutePath()+"/output.txt";
		this.outputfile = new File(filename);
		if(outputfile.exists()){
			boolean result = outputfile.delete();
			System.out.println("Output file "+outputfile.getAbsolutePath()+" exists. deleted = "+result);
		}
		
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
			FileWriter fw = new FileWriter(outputfile, true);
			BufferedWriter bw = new BufferedWriter(fw);
			out = new PrintWriter(bw);
			out.println(key+"\t"+value);
			keyswritten++;
			bw.close();
			fw.close();
			out.close();
		} catch (IOException e) {
			System.out.println("Could not write to output file");
		}
		
	}

}
