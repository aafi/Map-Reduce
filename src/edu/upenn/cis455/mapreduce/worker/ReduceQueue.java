package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Holds the arraylist of same words from the spool-in directory file
 * @author cis455
 *
 */
public class ReduceQueue {

	public static LinkedList <ArrayList<String>> reduceQueue = new LinkedList<ArrayList<String>>();
}
