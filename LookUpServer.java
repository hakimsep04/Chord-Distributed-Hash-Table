import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;

//import org.apache.commons.codec.digest.DigestUtils;

public class LookUpServer {
	
	public static void main(String[] args) throws IOException {
		ServerSocket serverSocket = new ServerSocket(5000);
		
		while(true) {
			System.out.println("waiting on port 5000");
			Socket clientSocket = serverSocket.accept();
			System.out.println("New node joined the network");
			ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
			ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
			Thread peerHandler = new LiveNodeHandler(clientSocket, inputStream, outputStream);
			peerHandler.start();
			
		}
	}
}


class LiveNodeHandler extends Thread{
	ObjectInputStream inputStream;
	ObjectOutputStream outputStream;
	Socket peerSocket;
	private static final int NODE_PORT = 8000;
	public static final int MAX_NODES = 16;
	private static TreeMap<Integer , InetAddress> liveNodes = new TreeMap<Integer , InetAddress>();
	
	public LiveNodeHandler(Socket socket, ObjectInputStream is, ObjectOutputStream os) throws IOException {
		this.peerSocket = socket;
		this.inputStream = is;
		this.outputStream = os;
	}
	
	public void run(){
		
		try {
			switch ((String)inputStream.readObject()) {
			case "Joining":
				nodeJoiningNetwork();
				break;

			case "Going offline":
				nodeLeavingNetwork();
				break;
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
		}

	}
	
	public void nodeJoiningNetwork() {
		try {
			InetAddress peerIP = peerSocket.getInetAddress();
//			System.out.println(peerIP.toString());
//			String sha = DigestUtils.sha1Hex(peerIP.toString());
//			System.out.println("sha1 = "+sha);
//			
//			BigInteger maxNodes = new BigInteger("16");
			int nodeID = (int)inputStream.readObject();
			System.out.println(nodeID);
			synchronized (liveNodes) {
				liveNodes.put(nodeID, peerIP);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			sendLiveNodes();
		}
	}
	
	public void nodeLeavingNetwork() {
		try {
			InetAddress peerIP = peerSocket.getInetAddress();
			System.out.println(peerIP.toString());
			int nodeID = (int) inputStream.readObject();
			synchronized (liveNodes) {
				liveNodes.remove(nodeID);
			}
			sendLiveNodes();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
		}
	}
	
	public void sendLiveNodes() {
		try {
			for(Map.Entry<Integer, InetAddress> entry: liveNodes.entrySet() ) {
				InetAddress nodeIP = entry.getValue();
				System.out.println("Sending livenodes list to " + nodeIP.toString());
				Socket socket = new Socket(nodeIP, NODE_PORT);
				outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject("Finger Table");
				outputStream.writeObject(liveNodes);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
		}
	}
}