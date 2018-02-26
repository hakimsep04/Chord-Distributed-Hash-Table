import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;

public class LookUpServer {
	
	public static void main(String[] args) throws IOException {
		ServerSocket serverSocket = new ServerSocket(5000);
		
		while(true) {
			System.out.println("waiting on port 5000");
			Socket clientSocket = serverSocket.accept();
			System.out.println("New node joined the network");

			System.out.println("Before os");
			ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
			ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
			System.out.println("client");
			Thread peerHandler = new LiveNodeHandler(clientSocket, inputStream, outputStream);
			peerHandler.start();
			
		}
	}
}


class LiveNodeHandler extends Thread{
	final ObjectInputStream inputStream;
	final ObjectOutputStream outputStream;
	final Socket peerSocket;
	public static final int MAX_NODES = 16;
	private static TreeMap<Integer , InetAddress> liveNodes = new TreeMap<Integer , InetAddress>();
	
	public LiveNodeHandler(Socket socket, ObjectInputStream is, ObjectOutputStream os) throws IOException {
		this.peerSocket = socket;
		this.inputStream = is;
		this.outputStream = os;
	}
	
	public void run(){
		InetAddress peerIP = peerSocket.getInetAddress();
		System.out.println(peerIP.toString());
		String sha = DigestUtils.sha1Hex(peerIP.toString());
		System.out.println("sha1 = "+sha);
		
		BigInteger maxNodes = new BigInteger("16");
		int nodeID = new BigInteger(sha, 16).mod(maxNodes).intValue();
		System.out.println(nodeID);
		synchronized (liveNodes) {
			liveNodes.put(nodeID, peerIP);
		}
		try {
			outputStream.writeObject(nodeID);
			outputStream.writeObject(liveNodes);
			System.out.println((String)inputStream.readObject());
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}