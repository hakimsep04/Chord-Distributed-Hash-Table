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
		// TODO Auto-generated constructor stub
		this.guid = guid;
		this.host = host;
		files = new ArrayList<String>();
		listeningThread = new Thread(this);
		faultToleranceHandler = new FaultToleranceHandler(this);
		listeningThread.setDaemon(true);
		listeningThread.start();
	}

	public static void main(String[] args) throws ClassNotFoundException {
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
		Runtime.getRuntime().addShutdownHook(node.faultToleranceHandler);
//		Runtime.getRuntime().addShutdownHook(new Thread()
//	    {
//	      public void run()
//	      {
//	        System.out.println("Shutdown Hook is running !");
//	      }
//	    });
		menu(node);
		
		
		

	}

	public static void menu(Peer node) {
		while (true) {
			System.out.println("********************** MENU BEGIN ************************");
			System.out.println("\t\t" +
					"1. Join the network \n 2. Leave the network \n 3. Insert file \n 4. Search file \n 5. Show finger table \n 6. Show files in this machine");
			System.out.println("********************** MENU END ************************");
			Scanner scan = new Scanner(System.in);
			try {
				switch (scan.nextInt()) {
				case 1:
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
					if (node.isOnline) {
						System.out.println("Please enter file name");
						String searchFile = scan.next();
						if (node.files.contains(searchFile)) {
							System.out.println("File " + searchFile + " found at " + node.guid);
						} else {
							int lookUpID = Math.abs(searchFile.hashCode() % MAX_NODES);
							if (lookUpID == node.guid) {
								System.out.println("Error: File not found!");
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
					System.out.println("Printing finger table");
					if (node.isOnline) {
						node.fingerTable.printFingerTable();
					} else {
						System.err.println("Node is offline");
					}
					break;
				case 6:
					System.out.println("********************** FILES ************************");
					if (node.files.size() == 0) {
						System.out.println("No files in this machine!");
					} else {
						System.out.println("There are " + node.files.size() + " files in this machine");
						for (String fileName : node.files) {
							System.out.println("\t\t" + fileName);
						}
					}
					System.out.println("********************** FILES ************************");
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

	public void enterNetwork() {
		try {
			connect();
			outputStream.writeObject("Joining");
			outputStream.writeObject(guid);
			System.out.println((String)inputstream.readObject());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeConnection();
		}
	}

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

	public void constructFingerTable() {
		fingerTable = new FingerTable(4, guid);
		fingerTable.constructFingerTable(liveNodes);
	}

	public void searchFile(InetAddress sourceAddress, int id, String file) {
		try {
			boolean isNodeAvailable = fingerTable.successorTable.containsKey(id);
			if (isNodeAvailable) {
				int successorId = fingerTable.successorTable.get(id);
				queryNodeForFile(sourceAddress, successorId, file, successorId);
			} else {

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
				if (checkBetweenNodes(idToSend, successorId, id)) {
					queryNodeForFile(sourceAddress, successorId, file, successorId);
				} else {
					queryNodeForFile(sourceAddress, successorId, file, id);
				}

			}
		} finally {

		}
	}

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

	public void transferFileToNode(int idToBeInserted, int successorID, String file, boolean isFile) {
		try {
			InetAddress successorIP = liveNodes.get(successorID);
			Socket socket = new Socket(successorIP, LISTENING_PORT);
			ObjectOutputStream opStream = new ObjectOutputStream(socket.getOutputStream());
			if (isFile) {
				opStream.writeObject("Insert file");
				System.out.println("File : " + file + " = " + Math.abs(file.hashCode()%16) + " routed to : " + successorID);
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
					while(iter.hasNext()) {
						String fileToBeAdded = iter.next();
						int fileID = Math.abs(fileToBeAdded.hashCode()%16);
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

class FaultToleranceHandler extends Thread{
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
		if(node.isOnline) {
			node.FaultToleranceLeaveNetwork(successorID);
		}
		System.out.println("Unexpected shutdown");
		
	}
	
}

