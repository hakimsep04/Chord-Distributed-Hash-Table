import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;

/*
 * @author: Abdul Hakim Shanavas
 * Server class for keeping track of live nodes in the network and updating the list to all the nodes in the network
 */

public class LookUpServer {

	public static void main(String[] args) throws IOException {
		ServerSocket serverSocket = new ServerSocket(5000);

		while (true) {
			System.out.println("waiting on port 5000");
			Socket clientSocket = serverSocket.accept();
			System.out.println("New node joined the network");
			ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
			ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
			// Routes all requests to the node handler
			Thread peerHandler = new LiveNodeHandler(clientSocket, inputStream, outputStream);
			peerHandler.start();

		}
	}
}

class LiveNodeHandler extends Thread {
	ObjectInputStream inputStream;
	ObjectOutputStream outputStream;
	Socket peerSocket;
	private static final int NODE_PORT = 8000;
	public static final int MAX_NODES = 16;
	private static TreeMap<Integer, InetAddress> liveNodes = new TreeMap<Integer, InetAddress>();

	public LiveNodeHandler(Socket socket, ObjectInputStream is, ObjectOutputStream os) throws IOException {
		this.peerSocket = socket;
		this.inputStream = is;
		this.outputStream = os;
	}

	public void run() {
		// Node connects to server only for joining the network and for leaving the
		// network
		try {
			switch ((String) inputStream.readObject()) {
			case "Joining":
				nodeJoiningNetwork();
				break;

			case "Going offline":
				nodeLeavingNetwork();
				break;
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}

	}

	// Adds the node to the list of live nodes. If the node id is already online
	// then rejects the node with appropriate message.
	public void nodeJoiningNetwork() {
		try {
			InetAddress peerIP = peerSocket.getInetAddress();
			int nodeID = (int) inputStream.readObject();
			System.out.println(nodeID);
			synchronized (liveNodes) {
				if (!liveNodes.containsKey(nodeID)) {
					liveNodes.put(nodeID, peerIP);
					outputStream.writeObject("Welcome " + nodeID);
				} else {
					outputStream.writeObject(nodeID + " is already in use!");
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			sendLiveNodes();
		}
	}

	// Removes the node from the live nodes list and sends the updated live nodes
	// list to all the nodes in the network
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
	}

	// Sending live nodes to all the nodes in the network.
	public void sendLiveNodes() {
		try {
			for (Map.Entry<Integer, InetAddress> entry : liveNodes.entrySet()) {
				InetAddress nodeIP = entry.getValue();
				System.out.println("Sending livenodes list to " + nodeIP.toString());
				Socket socket = new Socket(nodeIP, NODE_PORT);
				outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject("Finger Table");
				outputStream.writeObject(liveNodes);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
	}
}