package loadbalancingbroker;

import java.util.concurrent.ThreadLocalRandom;

import loadbalancingbroker.broker.LBBroker;
import loadbalancingbroker.client.Client;
import loadbalancingbroker.worker.Worker;

public class Main {
	private static final String FRONTEND_URL = "tcp://localhost:5555";
	private static final int NBR_CLIENTS = 1;

	private static final String BACKEND_URL = "tcp://localhost:6666";
	private static final int NBR_WORKERS = 50;
	private static final int MIN_IMAGE_DIMENSION = 600;
	private static final int MAX_IMAGE_DIMENSION = 700;
	private static class ClientThread extends Thread {
		public void run() {
			// hier is random dimension 
			// of image that should be painted by client
			int randWidthHeight = ThreadLocalRandom.current().
					nextInt(MIN_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION+1);
			final Client c = new Client(FRONTEND_URL,  randWidthHeight);
			c.start();
		}
	}

	private static class WorkerThread extends Thread {
		public void run() {
			final Worker w = new Worker(BACKEND_URL);
			w.start();
		}
	}

	private static class BrokerThread extends Thread {
		public void run() {
			final LBBroker b = new LBBroker(FRONTEND_URL, BACKEND_URL);
			b.start();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		BrokerThread brokerThread = new BrokerThread();
		brokerThread.start();

		WorkerThread[] workerThreads = new WorkerThread[NBR_WORKERS];
		for (int workerNbr = 0; workerNbr < NBR_WORKERS; workerNbr++) {
			final WorkerThread workerThread = new WorkerThread();
			workerThreads[workerNbr] = workerThread;
			workerThread.start();
		}

		ClientThread[] clientThreads = new ClientThread[NBR_CLIENTS];
		for (int clientNbr = 0; clientNbr < NBR_CLIENTS; clientNbr++) {
			final ClientThread clientThread = new ClientThread();
			clientThreads[clientNbr] = clientThread;
			clientThread.start();
		}

		for (int clientNbr = 0; clientNbr < NBR_CLIENTS; clientNbr++) {
			final ClientThread clientThread = clientThreads[clientNbr];
			clientThread.join();
		}
		
		for (int workerNbr = 0; workerNbr < NBR_WORKERS; workerNbr++) {
			final WorkerThread workerThread = workerThreads[workerNbr];
			workerThread.join();
		}
		
		brokerThread.join();
	}
}
