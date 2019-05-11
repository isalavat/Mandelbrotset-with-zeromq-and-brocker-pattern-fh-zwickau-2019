// Doc: http://zguide.zeromq.org/page:all#A-Load-Balancing-Message-Broker
// Source: http://zguide.zeromq.org/java:lbbroker

package loadbalancingbroker.broker;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class LBBroker {
	private final String fronendURL;
	private final String backendURL;
	private boolean started = false;

	public LBBroker(String frontendURL, String backendURL) {
		this.fronendURL = frontendURL;
		this.backendURL = backendURL;
	}

	/**
	 * This is the broker's main task. It routes messages between clients and workers. Workers signal READY when they
	 * start; after that we treat them as ready when they reply with a response back to a client. The load-balancing
	 * data structure is just a SortedSet of workers sorted by benchmark value (Preformance Index).
	 */
	public synchronized void start() {
		if (started) {
			throw new IllegalStateException("Broker already started.");
		}
		started = true;
		
		try (Context context = ZMQ.context(1);
				// Prepare our context and sockets
				Socket frontend = context.socket(SocketType.ROUTER);
				Socket backend = context.socket(SocketType.ROUTER)) {
			frontend.bind(fronendURL);
			backend.bind(backendURL);

			// Here is the main loop for the load-balancing-sorted set. It has two sockets:
			// - a frontend for clients and
			// - a backend for workers.
			//
			// It polls the backend in all cases, and polls the frontend only when there
			// are one or more workers ready. as next available worker for client request 
			// It will be chosen the worker with highest benchmark-value
			//
			// When we get a client request, we pop the next available worker with highest 
			// benchmark value from the sorted set of available workers, 
			// and send the request to this worker. The request
			// message includes the originating client identity.
			// When a worker replies, we re-add that worker, and we forward the reply
			// to the original client, using the reply envelope.
			

			// comparator for sorting by benchmark value of WorkerIDWrapper 
			Comparator<WorkerIDWrapper> comparator = (w1, w2) -> {
				return w1.getWorkerBenchmark().
					   compareTo(w2.getWorkerBenchmark());
			};
			
			// Tree set of available workers, sorted by benchmark value, 
			// that tells about performance speed of worker 
			TreeSet<WorkerIDWrapper> workerTree = new TreeSet<>(comparator);

			while (!Thread.currentThread().isInterrupted()) {
				// Initialize poll set
				Poller items = context.poller(2);
				// Always poll for worker activity on backend
				int backendPollerId = items.register(backend, Poller.POLLIN);
				// Poll front-end only if we have available workers
				int frontendPollerId = -1;
				if (workerTree.size() > 0)
					frontendPollerId = items.register(frontend, Poller.POLLIN);

				if (items.poll() < 0)
					break;

				// handle worker activity on backend
				if (items.pollin(backendPollerId)) {
					//Properties for a new WorkerIdWrapper
	                final String workerId = backend.recvStr();
				    Integer workerBenchmark = null;

					{
						// second frame is always empty
						final String empty = backend.recvStr();
						assert (empty.length() == 0);
					}

					{
						// third frame is "READY" and benchmark or else a client reply ID
						final String clientId = backend.recvStr();
						if(clientId.contains("READY")){
							// split and retrieve a benchmark value 
							workerBenchmark = Integer.valueOf(clientId.split(",")[1]);
						} 
						// if client reply, send rest of message back to frontend
						else if (!clientId.equals("READY")) {
							{
								// forth frame is empty
								final String empty = backend.recvStr();
								assert (empty.length() == 0);
							}
							
							{
								workerBenchmark = Integer.valueOf(backend.recvStr());
							}
							
							{
							//fifth frame is empty
							final String empty = backend.recvStr();
							}
							
							{
							// sixth frame is worker's reply to be passed on to client
							final byte[] replyBytes = backend.recv();
							// pass worker's reply on the client with given ID
							frontend.sendMore(clientId);
							frontend.sendMore("");
							frontend.send(replyBytes);
							}
						}
						// create and add the available worker to sorted 
						// set sorted by benchmark of each worker (see comparator on the top ^)
						workerTree.add(new WorkerIDWrapper(workerId, workerBenchmark));
					}
				}

				if (items.pollin(frontendPollerId)) {
					// Now get next client request and route it to LRU worker;
					// Client request is [address][empty][request]
					final String clientId = frontend.recvStr();

					{
						// Second frame is always empty
						final String empty = frontend.recvStr();
						assert (empty.length() == 0);
					}

					final byte[] requestBytes = frontend.recv();

					WorkerIDWrapper workerIDWrapper = workerTree.pollLast(); 
					final String workerId = workerIDWrapper.getWorkerID();

					// pass client's request on this worker
					backend.sendMore(workerId);
					backend.sendMore("");
					backend.sendMore(clientId);
					backend.sendMore("");
					backend.send(requestBytes);
					//remove the worker wrapped by workerIDWrapper from sorted set
					workerTree.remove(workerIDWrapper);
				}
			}
		} finally {
			started = false;
		}
	}
	
	/**
	 * Wraps two properties of Worker 
	 * to give the ability to sort WorkerIds by benchmark value
	 * @author salavat
	 */
	class WorkerIDWrapper {
		private String workerID;
		private Integer workerBenchmark;
		
		public WorkerIDWrapper() {}
		
		public WorkerIDWrapper(String workerID, Integer workerBenchmark) {
			this.workerID = workerID;
			this.workerBenchmark = workerBenchmark;
		}
		
		public WorkerIDWrapper(String workerID) {
			this.workerID = workerID;
			this.workerBenchmark = null;
		}
		
		public String getWorkerID() {
			return workerID;
		}
		public Integer getWorkerBenchmark() {
			return workerBenchmark;
		}

		public void setWorkerID(String workerID) {
			this.workerID = workerID;
		}

		public void setWorkerBenchmark(Integer workerBenchmark) {
			this.workerBenchmark = workerBenchmark;
		}
		
	}
}
