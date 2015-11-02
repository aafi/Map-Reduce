package edu.upenn.cis455.mapreduce.worker;

/**
 * The Class ThreadpoolThread.
 */
	
public class ThreadpoolThread {

	
	/** The thread */
	private Thread thread;
	private MapWorker map_worker;
	private ReduceWorker reduce_worker;
		
	/**
	 * Instantiates a new threadpool thread for map.
	 *
	 * @param worker the worker object
	 */
	public ThreadpoolThread(MapWorker worker){
		this.thread = new Thread(worker);
		this.map_worker = worker;
		this.thread.start();
	}
	
	/**
	 * Instantiates a new threadpool thread for reduce.
	 *
	 * @param worker the worker object
	 */
	public ThreadpoolThread(ReduceWorker worker){
		this.thread = new Thread(worker);
		this.reduce_worker = worker;
		this.thread.start();
	}

	/**
	 * Gets the thread.
	 *
	 * @return the thread
	 */
	public Thread getThread() {
		return thread;
	}

	/**
	 * Gets the map worker.
	 *
	 * @return the worker
	 */
	public MapWorker getMapWorker() {
		return map_worker;
	}
	
	/**
	 * Gets the reduce worker.
	 *
	 * @return the worker
	 */
	public ReduceWorker getReduceWorker() {
		return reduce_worker;
	}
		
}
