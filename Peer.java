import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

/*
 * @author: Abdul Hakim Shanavas
 * Rochester Institute of Technology
 * 
 * Peer class is the node in the network. This class communicates with LookUp server for joining a network. 
 * Besides that, all communication is only between nodes for file insertion, retrieval.  
 */

public class Peer implements Runnable {
	private ObjectInputStream inputstream;
	private ObjectOutputStream outputStream;
	private Socket nodeSocket;
	private String host;
	private int guid;
	public boolean isOnline = false;
	private boolean isFirst = false;
	private FingerTable fingerTable;
	private FaultToleranceHandler faultToleranceHandler;
	private Thread listeningThread;
	private TreeMap<Integer, InetAddress> liveNodes;
	private static final int PORT = 5000;
	private static final int LISTENING_PORT = 8000;
	private static final int MAX_NODES = 16;
	private ArrayList<String> files;

	public Peer(int guid, String host) {
		this.guid = guid;
		this.host = host;
		files = new ArrayList<String>();
		listeningThread = new Thread(this);
		faultToleranceHandler = new FaultToleranceHandler(this);
		listeningThread.setDaemon(true);
		listeningThread.start();
	}

	public static void main(String[] args) throws ClassNotFoundException {
		// Check for command line arguments
		if (args.length < 2) {
			System.out.println("Please provide guid and network host");
			System.exit(11);
		}
		int id = Integer.parseInt(args[0]);
		if (id < 0 || id > 15) {
			System.out.println("Please give node id between 0 and 15 inclusive");
			System.exit(11);
		}
		Peer node = new Peer(id, args[1]);
		// Handling fault tolerance. This will get triggered when the JVM exists
		// unexpectedly ( Eg.: Ctrl-C )
		Runtime.getRuntime().addShutdownHook(node.faultToleranceHandler);
		// Main menu
		menu(node);
	}

	public static void menu(Peer node) {
		while (true) {
			System.out.println("********************** MENU BEGIN ************************");
			System.out.println("\t\t"
					+ "1. Join the network \n \t\t2. Leave the network \n \t\t3. Insert file \n \t\t4. Search file \n \t\t5. Show finger table \n \t\t6. Show files in this machine");
			System.out.println("********************** MENU END ************************");
			Scanner scan = new Scanner(System.in);
			try {
				switch (scan.nextInt()) {
				case 1:
					// Join network only if the node is offline.
					if (!node.isOnline) {
						node.enterNetwork();
						System.out.println("Joined the network");
						System.out.println("Waiting for livenodes list");
						node.isOnline = true;
						node.isFirst = true;
					} else {
						System.out.println("Node is already online");
					}

					break;
				case 2:
					// Leave network only when the node is already online
					if (node.isOnline) {
						node.leaveNetwork();
						node.isOnline = false;
						System.out.println("Left the network");
						node.files.clear();
					} else {
						System.out.println("Node is already offline");
					}
					break;
				case 3:
					// Insert file into the network by matching the key(hashed file) to guid
					if (node.isOnline) {
						System.out.println("Please enter file name");
						String file = scan.next();
						int idToBeInserted = Math.abs(file.hashCode() % MAX_NODES);
						System.out.println("File id: " + idToBeInserted);
						node.insertFileAtID(idToBeInserted, file);
					} else {
						System.out.println("Node is offline! Must be online to insert file");
					}
					break;
				case 4:
					// Search for a file. First locally then check in the network.
					if (node.isOnline) {
						System.out.println("Please enter file name");
						String searchFile = scan.next();
						// Checking for files in the local machine
						if (node.files.contains(searchFile)) {
							System.out.println("File " + searchFile + " found at " + node.guid);
						} else {
							// If the file supposed to be in the local machine but it is not found, then
							// display file not found
							int lookUpID = Math.abs(searchFile.hashCode() % MAX_NODES);
							if (lookUpID == node.guid) {
								System.err.println("Error: File not found!");
							} else {
								System.out.println("File id: " + lookUpID + " for lookup");
								InetAddress sourceAddress = InetAddress.getLocalHost();
								node.searchFile(sourceAddress, lookUpID, searchFile);
							}
						}
					} else {
						System.out.println("Node is offline! Cannot connect to network!");
					}
					break;
				case 5:
					// Prints the finger table
					System.out.println("Printing finger table");
					if (node.isOnline) {
						node.fingerTable.printFingerTable();
					} else {
						System.err.println("Node is offline");
					}
					break;
				case 6:
					// Shows the file present in the local machine
					System.out.println("********************** FILES ************************");
					if (node.files.size() == 0) {
						System.out.println("No files in this machine!");
					} else {
						System.out.println("There are " + node.files.size() + " files in this machine");
						for (String fileName : node.files) {
							System.out.println("\t\t" + fileName);
						}
					}
					System.out.println("********************** FILES END ************************");
				}

			} catch (InputMismatchException e) {
				System.err.println("Invalid input");

			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {

			}
		}
	}

	// Connects to LookUp server
	public void connect() {
		try {
			InetAddress serverIP = InetAddress.getByName(host);
			nodeSocket = new Socket(serverIP, PORT);
			inputstream = new ObjectInputStream(nodeSocket.getInputStream());
			outputStream = new ObjectOutputStream(nodeSocket.getOutputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Close the connection between node and the LookUp server
	public void closeConnection() {
		try {
			outputStream.close();
			inputstream.close();
			nodeSocket.close();
		} catch (IOException e) {
			System.err.println("Connection Error");
			System.exit(1);
		}
	}

	// Connect to server and go online
	public void enterNetwork() {
		try {
			connect();
			outputStream.writeObject("Joining");
			outputStream.writeObject(guid);
			System.out.println((String) inputstream.readObject());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeConnection();
		}
	}

	// Connect to server and go offline. Also send all the files in the system to
	// the immediate successor.
	public void leaveNetwork() {
		try {
			connect();
			outputStream.writeObject("Going offline");
			outputStream.writeObject(guid);
			int id = fingerTable.firstActualNode;
			int successor = fingerTable.successorTable.get(id);
			transferFileToNode(successor, successor, "", false);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeConnection();
		}
	}

	// Transfer files to immediate successor when node shuts down unexpectedly
	public void FaultToleranceLeaveNetwork(int successor) {
		try {
			connect();
			outputStream.writeObject("Going offline");
			outputStream.writeObject(guid);
			transferFileToNode(successor, successor, "", false);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeConnection();
		}
	}

	// Constructs the finger table based on the live nodes in the network
	public void constructFingerTable() {
		fingerTable = new FingerTable(4, guid);
		fingerTable.constructFingerTable(liveNodes);
	}

	// Search file based in the network and send the source address for the node
	// which has file to directly connect and return the file
	public void searchFile(InetAddress sourceAddress, int id, String file) {
		try {
			boolean isNodeAvailable = fingerTable.successorTable.containsKey(id);
			// First checks the file's owner in the finger table. If the node in the finger
			// table is supposed to have the file, then query to it's successor for file.
			if (isNodeAvailable) {
				int successorId = fingerTable.successorTable.get(id);
				queryNodeForFile(sourceAddress, successorId, file, successorId);
			} else {
				// If the node is not in the finger table, query the closest node for the file
				// which will in turn check it's finger table and send query to the actual
				// successor
				int sourceID;
				int minDistance = Integer.MAX_VALUE;
				TreeMap<Integer, Integer> nodesDistance = new TreeMap<Integer, Integer>();
				for (Map.Entry<Integer, Integer> entry : fingerTable.successorTable.entrySet()) {
					sourceID = entry.getKey();
					int distance = distanceBetweenNodes(sourceID, id);
					minDistance = Math.min(minDistance, distance);
					nodesDistance.put(distance, sourceID);
				}
				int idToSend = nodesDistance.get(minDistance);
				int successorId = fingerTable.successorTable.get(idToSend);
				// If the node to be searched is between the actual and successor node then
				// query the successor for the file, else query the successor to query the
				// actual node which has the file.
				if (checkBetweenNodes(idToSend, successorId, id)) {
					queryNodeForFile(sourceAddress, successorId, file, successorId);
				} else {
					queryNodeForFile(sourceAddress, successorId, file, id);
				}
			}
		} finally {

		}
	}

	//Method which is responsible for querying the nodes in the network for searching a file
	public void queryNodeForFile(InetAddress sourceAddress, int successorId, String file, int fileID) {
		try {
			InetAddress successorIP = liveNodes.get(successorId);
			System.out.println("Querying for file : " + file + " to node : " + successorId);
			Socket socket = new Socket(successorIP, LISTENING_PORT);
			ObjectOutputStream opStream = new ObjectOutputStream(socket.getOutputStream());
			opStream.writeObject("Query file");
			opStream.writeObject(sourceAddress);
			opStream.writeObject(file);
			opStream.writeObject(fileID);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
	}

	//Same as the searching file, except that it has to insert the file in that particular node.
	public void insertFileAtID(int id, String file) {
		if (id == guid) {
			System.out.println("File : " + file + " inserted at : " + guid);
			files.add(file);
		} else {
			boolean isNodeAvailable = fingerTable.successorTable.containsKey(id);
			if (isNodeAvailable) {
				int successorID = fingerTable.successorTable.get(id);
				if (successorID == guid) {
					files.add(file);
					System.out.println("File = " + file + " inserted at " + guid);
				} else {
					transferFileToNode(successorID, successorID, file, true);
				}

			} else {
				int sourceID;
				int minDistance = 20;
				TreeMap<Integer, Integer> nodesDistance = new TreeMap<Integer, Integer>();
				for (Map.Entry<Integer, Integer> entry : fingerTable.successorTable.entrySet()) {
					sourceID = entry.getKey();
					int distance = distanceBetweenNodes(sourceID, id);
					minDistance = Math.min(minDistance, distance);
					nodesDistance.put(distance, sourceID);
				}
				int idToSend = nodesDistance.get(minDistance);
				int successorId = fingerTable.successorTable.get(idToSend);
				if (successorId == guid) {
					files.add(file);
					System.out.println("File = " + file + " inserted at " + guid);
				} else {
					if (checkBetweenNodes(idToSend, successorId, id)) {
						transferFileToNode(successorId, successorId, file, true);
					} else {
						transferFileToNode(id, successorId, file, true);
					}
				}

			}

		}

	}

	//Returns true if the node looking for is between the actual and successor node.
	public boolean checkBetweenNodes(int startNode, int endNode, int actualNode) {
		int i = startNode;
		boolean isBetween = false;
		while (i != endNode) {
			i = Math.abs((i + 1) % 16);
			if (i == actualNode) {
				isBetween = true;
				break;
			}
		}
		return isBetween;
	}

	//Finds the distance between any two nodes in the network
	public int distanceBetweenNodes(int sourceId, int destId) {
		int distance = 1;
		int result;
		for (;;) {
			result = (distance + sourceId) % MAX_NODES;
			if (result == destId) {
				break;
			}
			distance++;
		}
		return distance;
	}

	//Transfers files from local to the respective node in the network
	public void transferFileToNode(int idToBeInserted, int successorID, String file, boolean isFile) {
		try {
			InetAddress successorIP = liveNodes.get(successorID);
			Socket socket = new Socket(successorIP, LISTENING_PORT);
			ObjectOutputStream opStream = new ObjectOutputStream(socket.getOutputStream());
			if (isFile) {
				opStream.writeObject("Insert file");
				System.out.println(
						"File : " + file + " = " + Math.abs(file.hashCode() % 16) + " routed to : " + successorID);
				opStream.writeObject(idToBeInserted);
				opStream.writeObject(file);
			} else {
				opStream.writeObject("Transferring files");
				opStream.writeObject(files);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
	}

	//Get files from the successor when a node comes online
	@SuppressWarnings("unchecked")
	public void getFilesFromSuccessor() {
		try {
			if (isOnline) {
				int id = fingerTable.firstActualNode;
				int successorID = fingerTable.successorTable.get(id);
				if (successorID != guid) {
					System.out.println("Getting back files from successor");
					InetAddress successorIP = liveNodes.get(successorID);
					Socket socket = new Socket(successorIP, LISTENING_PORT);
					System.out.println("Before ops");
					ObjectOutputStream opStream = new ObjectOutputStream(socket.getOutputStream());
					opStream.writeObject("Get files");
					ObjectInputStream ipStream = new ObjectInputStream(socket.getInputStream());
					opStream.writeObject(guid);
					ArrayList<String> tempFiles = (ArrayList<String>) ipStream.readObject();
					for (String string : tempFiles) {
						files.add(string);
					}
					System.out.println("All files received from successor");
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
	}

	//Catching the response for the query sent for searching a file in the network. 
	public void searchFileQueryResponse(String message, InetAddress sourceAddress) {
		try {
			Socket socket = new Socket(sourceAddress, LISTENING_PORT);
			ObjectOutputStream opStream = new ObjectOutputStream(socket.getOutputStream());
			opStream.writeObject("Query response");
			opStream.writeObject(message);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
	}
	
	//Thread which listens to all the incoming calls from other nodes and server in the network
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			ServerSocket listeningSocket = new ServerSocket(LISTENING_PORT);
			while (true) {
				Socket socket = listeningSocket.accept();
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				switch ((String) inputStream.readObject()) {
				case "Finger Table":
					liveNodes = (TreeMap<Integer, InetAddress>) inputStream.readObject();
					System.out.println("Received livenodes");
					constructFingerTable();
					faultToleranceHandler.successorID = fingerTable.successorTable.get(fingerTable.firstActualNode);
					if (isFirst) {
						isFirst = false;
						getFilesFromSuccessor();
					}
					break;
				case "Insert file":
					int id = (int) inputStream.readObject();
					String file = (String) inputStream.readObject();
					System.out.println("Insert file at :" + id);
					System.out.println("File: " + file);
					insertFileAtID(id, file);
					break;
				case "Transferring files":
					ArrayList<String> tempFiles = (ArrayList<String>) inputStream.readObject();
					for (String string : tempFiles) {
						files.add(string);
					}
					System.out.println("Files received from predecessor");
					break;
				case "Get files":
					System.out.println("Giving back the files to predecessor");
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					int incomingId = (int) inputStream.readObject();
					ArrayList<String> preFiles = new ArrayList<String>();
					Iterator<String> iter = files.iterator();
					while (iter.hasNext()) {
						String fileToBeAdded = iter.next();
						int fileID = Math.abs(fileToBeAdded.hashCode() % 16);
						if (checkBetweenNodes(guid, incomingId, fileID)) {
							preFiles.add(fileToBeAdded);
							iter.remove();
						}
					}
					outputStream.writeObject(preFiles);
					System.out.println("All files transferred to predecessor");
					break;
				case "Query file":
					InetAddress sourceAddress = (InetAddress) inputStream.readObject();
					String searchFile = (String) inputStream.readObject();
					int fileID = (int) inputStream.readObject();
					String message;
					if (files.contains(searchFile)) {
						message = "File :" + searchFile + " found at :" + guid;
						searchFileQueryResponse(message, sourceAddress);
					} else if (fileID == guid) {
						message = "File :" + searchFile + " not found in the network";
						searchFileQueryResponse(message, sourceAddress);
					} else {
						searchFile(sourceAddress, fileID, searchFile);
					}
					break;
				case "Query response":
					System.out.println((String) inputStream.readObject());
					break;
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}

	}

}

//Fault tolerance thread. This thread gets triggered only when JVM shuts down unexpectedly. 
class FaultToleranceHandler extends Thread {
	public Peer node;
	public int successorID;

	public FaultToleranceHandler(Peer node) {
		// TODO Auto-generated constructor stub
		this.node = node;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("");
		if (node.isOnline) {
			node.FaultToleranceLeaveNetwork(successorID);
		}
		System.out.println("Unexpected shutdown");

	}

}
