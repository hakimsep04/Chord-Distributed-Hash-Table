import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class Peer implements Runnable {
	private ObjectInputStream inputstream;
	private ObjectOutputStream outputStream;
	private Socket nodeSocket;
	private String host;
	private int guid;
	private boolean isOnline = false;
	private boolean isFirst = false;
	private FingerTable fingerTable;
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
		menu(node);

	}

	public static void menu(Peer node) {
		while (true) {
			System.out.println(
					"1. Join the network \n 2. Leave the network\n 3. Insert file\n 4. Show finger table \n 5.Show files in this machine");
			Scanner scan = new Scanner(System.in);
			System.out.println("Welcome");
			try {
				switch (scan.nextInt()) {
				case 1:
					if(!node.isOnline) {
						node.enterNetwork();
						System.out.println("Joined the network");
						System.out.println("Waiting for livenodes list");
						node.isOnline = true;
						node.isFirst = true;
					}else {
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
					System.out.println("Printing finger table");
					if (node.isOnline) {
						node.fingerTable.printFingerTable();
					} else {
						System.err.println("Node is offline");
					}
					break;
				case 5:
					if (node.files.size() == 0) {
						System.out.println("No files in this machine!");
					} else {
						System.out.println("There are " + node.files.size() + " files in this machine");
						for (String fileName : node.files) {
							System.out.println("\t\t" + fileName);
						}
					}

				}

			} catch (InputMismatchException e) {
				System.out.println("Invalid input");

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
			System.out.println("Connection Error");
			System.exit(1);
		}
	}

	public void enterNetwork() {
		try {
			connect();
			outputStream.writeObject("Joining");
			outputStream.writeObject(guid);
			System.out.println("GUID : " + guid);
		} catch (IOException e) {
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
			int id = fingerTable.successorTable.firstKey();
			int successor = fingerTable.successorTable.get(id);
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

	public void insertFileAtID(int id, String file) {
		if (id == guid) {
			System.out.println("File :" + file + "inserted at :" + guid);
			files.add(file);
		} else {
			System.out.println("Inside else of insert file");
			boolean isNodeAvailable = fingerTable.successorTable.containsKey(id);
			if (isNodeAvailable) {
				int successorID = fingerTable.successorTable.get(id);
				if(successorID == guid) {
					files.add(file);
					System.out.println("File =" + file + " inserted at " + guid);
				}else {
					transferFileToNode(successorID, successorID, file, true);
				}
				
			} else {
				int sourceID;
				int minDistance = 20;
				System.out.println("Finding shortest distance between nodes");
				TreeMap<Integer, Integer> nodesDistance = new TreeMap<Integer, Integer>();
				for (Map.Entry<Integer, Integer> entry : fingerTable.successorTable.entrySet()) {
					sourceID = entry.getKey();
					int distance = distanceBetweenNodes(sourceID, id);
					minDistance = Math.min(minDistance, distance);
					nodesDistance.put(distance, sourceID);
				}
				System.out.println("Found the shortest distance");
				int idToSend = nodesDistance.get(minDistance);
				int successorId = fingerTable.successorTable.get(idToSend);
				if(successorId == guid) {
					files.add(file);
					System.out.println("File =" + file + " inserted at " + guid);
				}else {
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
				System.out.println("File :" + file + " = " + idToBeInserted + " routed to :" + successorID);
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
				int id = fingerTable.successorTable.firstKey();
				int successorID = fingerTable.successorTable.get(id);
				if(successorID != guid) {
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
				case "Find File":
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
					for (String string : files) {
						int fileID = Math.abs(string.hashCode());
						if (fileID > guid || fileID <= incomingId) {
							preFiles.add(string);
							files.remove(string);
						}
					}
					outputStream.writeObject(preFiles);
					System.out.println("All files transferred to predecessor");
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
