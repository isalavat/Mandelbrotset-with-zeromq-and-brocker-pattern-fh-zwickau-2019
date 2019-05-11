// Source:
// https://github.com/zeromq/jeromq/blob/master/src/test/java/guide/ZHelper.java

package loadbalancingbroker.zmqutils;

import java.util.Random;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ZHelper {
	private static Random rand = new Random(System.currentTimeMillis());

	/**
	 * Receives all message parts from socket, prints neatly
	 */
	public static void dump(Socket sock) {
		System.out.println("----------------------------------------");
		while (true) {
			byte[] msg = sock.recv(0);
			boolean isText = true;
			String data = "";
			for (int i = 0; i < msg.length; i++) {
				if (msg[i] < 32 || msg[i] > 127)
					isText = false;
				data += String.format("%02X", msg[i]);
			}
			if (isText)
				data = new String(msg, ZMQ.CHARSET);

			System.out.println(String.format("[%03d] %s", msg.length, data));
			if (!sock.hasReceiveMore())
				break;
		}
	}

	public static void setId(Socket sock) {
		String identity = String.format("%04X-%04X", rand.nextInt(),
				rand.nextInt());

		sock.setIdentity(identity.getBytes(ZMQ.CHARSET));
	}
}
