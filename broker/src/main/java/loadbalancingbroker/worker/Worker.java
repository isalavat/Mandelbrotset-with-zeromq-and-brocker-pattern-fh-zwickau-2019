// Doc: http://zguide.zeromq.org/page:all#A-Load-Balancing-Message-Broker
// Source: http://zguide.zeromq.org/java:lbbroker

package loadbalancingbroker.worker;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.google.protobuf.InvalidProtocolBufferException;

import loadbalancingbroker.domain.Complex;
import loadbalancingbroker.protobuf.ReplyProto.Reply;
import loadbalancingbroker.protobuf.ReplyProto.Reply.Row;
import loadbalancingbroker.protobuf.RequestProto.Request;
import loadbalancingbroker.zmqutils.ZHelper;

public class Worker {
	private final String url;
	private boolean started = false;
	private Integer benchmark;
	private final int ITERATION_NUM = 255;
	public Worker(String url) {
		this.url = url;
		this.benchmark = ThreadLocalRandom.current().nextInt(1, 11);
	}

	public synchronized void start() {
		if (started) {
			throw new IllegalStateException("Worker already started.");
		}
		started = true;
		try (Context context = ZMQ.context(1); //
				Socket worker = context.socket(SocketType.REQ)) {
			// Prepare our context and sockets
			ZHelper.setId(worker); // Set a printable identity
			final String id = new String(worker.getIdentity());
			System.out.println("Worker thread " + id + " started");

			// connect to back-end
			worker.connect(url);

			// Tell back-end we're ready for work 
			// and append to ready a benchmark of currentworker
			worker.send("READY"+","+benchmark);

			while (!Thread.currentThread().isInterrupted()) {
				String address = worker.recvStr();
				String empty = worker.recvStr();
				assert (empty.length() == 0);

				// Get request, send reply
				byte[] reqBytes = worker.recv();
				Request req = Request.parseFrom(reqBytes);
				int imgWidth = req.getImgWidth();
				int imgHeight = req.getImgHeight();
				int xBegin = req.getXBegin();
				int xEnd = req.getXEnd();
				int yBegin = req.getYBegin();
				int yEnd = req.getYEnd();
				double x0 = (3*imgWidth/4);
				double y0 = (imgHeight/2);
				double lengthOfXoY = imgWidth*0.47;
				System.out.println("Worker thread " + id + " with benchmark - "+benchmark+" recv from " + address);
				Reply rep = Reply.getDefaultInstance();
				
				rep.newBuilder();
				rep = rep.toBuilder().
				    	setXBegin(xBegin).
				    	setXEnd(xEnd).
				    	setYBegin(yBegin).
				    	setYEnd(yEnd).
				    	build();
			    for (int i = xBegin; i < xEnd; i++) {
			    	List<Integer> list = new ArrayList<>();
					for(int j = yBegin; j < yEnd; j++) {
		        		
		        		double x = (i-x0)/lengthOfXoY;
		        		double y = (j - y0)/lengthOfXoY;
		        		Complex z = new Complex(x, y);
		        		int gray = getGray(z); 
		        		list.add(gray);
					}
		        	rep = rep.toBuilder().addRows(Row.newBuilder().addAllGrayValues(list).build()).build();
		        }
				//System.out.println("Worker thread " + id + " with benchmark - "+benchmark+" recv from " + address + ": " + request);

				worker.sendMore(address);
				worker.sendMore("");
				worker.sendMore(String.valueOf(benchmark));
				worker.sendMore("");
				worker.send(rep.toByteArray());
			}
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			started = false;
		}
	}

	public Integer getBenchmark() {
		return benchmark;
	}

	public void setBenchmark(Integer benchmark) {
		this.benchmark = benchmark;
	}
	/**
	 * Calculates if complex number in mandelbrot set.
	 * Retruns grayscale color value exact for one pixel
	 * corresponding to give complex number 
	 * @param z0
	 * @return grayScaleColorValue
	 * @source http://zonakoda.ru/vizualizaciya-mnozhestva-mandelbrota.html
	 */
	private int getGray (Complex z0) {
		Complex z = new Complex(z0);
		for(int gray = ITERATION_NUM; gray >0; gray-- ) {
			if(z.abs() > 2)
				return Color.HSBtoRGB((float)gray / ITERATION_NUM, 0.5f, 1f);
			z = z.power(2).add(z0);
		}
		return Color.ORANGE.getRGB();
	}
}
