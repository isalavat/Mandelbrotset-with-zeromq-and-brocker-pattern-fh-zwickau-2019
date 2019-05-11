// Doc: http://zguide.zeromq.org/page:all#A-Load-Balancing-Message-Broker
// Source: http://zguide.zeromq.org/java:lbbroker

package loadbalancingbroker.client;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.google.protobuf.InvalidProtocolBufferException;

import loadbalancingbroker.protobuf.ReplyProto.Reply;
import loadbalancingbroker.protobuf.ReplyProto.Reply.Row;
import loadbalancingbroker.protobuf.RequestProto.Request;
import loadbalancingbroker.zmqutils.ZHelper;

/**
 * Assync request-reply client using REQ socket and Runnables(Threads).
 * This class splits tasks to paint an fractal image(mandelbrot) on JFrame
 */
public class Client {
	private final String url;
	private boolean started = false;
	private int countOfTasks;
	private int width;  // image width
	private int height; // image height
	private BufferedImage image = null;
	private JFrame frame;
	public Client(String url, int widthHeight) {
		this.url = url;
		this.width = widthHeight;
		this.height = widthHeight;
		this.countOfTasks = width; 
	}

	public synchronized void start() {
		if (started) {
			throw new IllegalStateException("Client already started.");
		}
		started = true;
		
		// init Window
		initJFrame();
		
		// create list of requests
		List<Request> reqs = constructReqs(width, height, countOfTasks);
		
		// create futures (callables) for all requests 
		// to be able to send async request and to retrieve reply     
		List<Runnable> runnables = constructRunnables(reqs);
		
		// start all runnables in this list
		runnables.forEach((task -> {
			Thread t = new Thread(task);	
			t.start();
		}));
		
		started = false;
	}
	
	/**
	 * initializes JFrame and adds to it buffered image
	 * of mandelbrot that has to be painted 
	 */
	private void initJFrame() {
		image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		frame = new JFrame("Mandelbrot");
	    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		ImageIcon imgIcon = new ImageIcon(image);
	    frame.add(new JLabel(imgIcon));
	    frame.setLocationByPlatform( true );
	    frame.pack();
	    frame.setVisible( true );
	}
	
	/**
	 * colors image pixels based on given reply
	 * @param reply
	 * @source http://zonakoda.ru/vizualizaciya-mnozhestva-mandelbrota.html
	 */
	private synchronized void processReply (Reply reply) {
		// variable that represents element index
		// from separate reply
		int p = -1; 
		for(int i = reply.getXBegin(); i < reply.getXEnd(); i++) {
			p++;
			Row row = reply.getRows(p);
			for(int k = 0; k < height; k++) {
				int gray = row.getGrayValues(k);
				//color the pixel on i,k coordinate and repaint the jframe
				image.setRGB(i, k, gray);
				frame.repaint();
				
			}
	  	}
	}
	
	/**
	 * Wraps all requests in Runnables 
	 * Runnables are responsible for sending and recieveing
	 * the req-reply also for painting mandelbrot image
	 * @param reqs
	 * @return runnables
	 */
	private List<Runnable> constructRunnables(List<Request> reqs) {
		List<Runnable> runnables = new ArrayList<>();
		for (Request req: reqs) {
			runnables.add(constructRunnable(req));
		}
		return runnables;
	}
	
	/**
	 * Wraps given request in a Runnable.
	 * The run method sends request, recieves 
	 * and processes reply
	 *  
	 * @param request
	 * @return runnable
	 */
	private Runnable constructRunnable(Request request) {
		Runnable task = () -> {
			try (Context context = ZMQ.context(1); //
					Socket client = context.socket(SocketType.REQ)) {
				ZHelper.setId(client); // Set a printable identity
				final String id = new String(client.getIdentity());
				System.out.println("Client thread " + id + " started");
				// connect to front-end
				client.connect(url);
				//send request and receive reply
				client.send(request.toByteArray());
				byte[] replyBytes = client.recv();
				Reply reply;
				try {
					reply = Reply.parseFrom(replyBytes);
					processReply(reply);
				} catch (InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				client.close();
				System.out.println("Client thread " + id + " terminated");
				
			}
		};
		return task;
	} 

	/**
	 * creates given count identical Requests, 
	 * by splitting the task based on image dimension  
	 * @param imgWidth
	 * @param imgHeight
	 * @param count - count of request or in how much parts should be  this image splitted
	 * @return
	 */
	private List<Request> constructReqs(int imgWidth, int imgHeight, int count){
		int imgPortionOnX = imgWidth/count;
		List<Request> reqs = new ArrayList<>();
		for(int i = 0; i<count; i++) {
			reqs.add(constructReq(imgWidth, imgHeight, imgPortionOnX, i));
		}
		return reqs;
	}
	
	/**
	 * create one Request for calculating of defined image part  
	 * @param imgWidth
	 * @param imgHeight
	 * @param imgPortionOnX - width of image part
	 * @param order - exactly which part
	 * @return
	 */
	private Request constructReq(int imgWidth, int imgHeight, int imgPortionOnX, int order) {
		int xBegin = imgPortionOnX*order;
		Request req = Request.newBuilder().
				setImgWidth(width).
				setImgHeight(height).
				setXBegin(xBegin).
				setXEnd(xBegin + imgPortionOnX).
				setYBegin(0).
				setYEnd(height).
				build();
		return req;
	}
}
