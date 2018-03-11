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
	private boolean isListeningThread = false;
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
		this.isListeningThread = true;
		listeningThread.start();
	}

	public static void main(String[] args) throws ClassNotFoundException {
		if (args.length < 2) {
			System.out.println("Please provide guid and network host");
			System.exit(11);
		}
		int id = Integer.parseInt(args[0]);
		if(id < 0 || id > 15) {
			System.out.println("Please give node id between 0 and 15 inclusive");
			System.exit(11);
		}
		Peer node = new Peer(id, args[1]);
		menu(node);

	}

	public static void menu(Peer node) {
		while (true) {
			System.out.println("1. Join the network \n 2. Leave the network\n 3. Insert file\n 4. Show finger table");
			Scanner scan = new Scanner(System.in);
			System.out.println("Welcome");
			try {
				switch (scan.nextInt()) {
				case 1:
					node.enterNetwork();
					//node.constructFingerTable();
					System.out.println("Joined the network");
					break;
				case 2:
					node.leaveNetwork();
					System.out.println("Left the network");
					break;
				case 3:
					System.out.println("Please enter file name");
					String file = scan.next();
					int idToBeInserted = Math.abs(file.hashCode() % MAX_NODES );
					System.out.println("File id: " + idToBeInserted);
					node.insertFileAtID(idToBeInserted, file);
					break;
				case 4:
					System.out.println("Printing finger table");
					node.fingerTable.printFingerTable();
					break;
				}

			} catch (InputMismatchException e) {
				// TODO: handle exception
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	 public void closeConnection(){
	        try {
	            outputStream.close();
	            inputstream.close();
	            nodeSocket.close();
	        }
	        catch(IOException e){
	            System.out.println("Connection Error");
	            System.exit(1);
	        }
	    }
	
	public void enterNetwork() {
		try {
			connect();
			outputStream.writeObject("Joining");
			outputStream.writeObject(guid);
			//liveNodes = (TreeMap<Integer, InetAddress>) inputstream.readObject();
			System.out.println("GUID : " + guid);
			//System.out.println("Size : " + liveNodes.size());
			//outputStream.writeObject("Received livenodes");
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			closeConnection();
		}
	}
	
	public void leaveNetwork() {
		try {
			connect();
			outputStream.writeObject("Going offline");
			outputStream.writeObject(guid);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			closeConnection();
		}
	}

	public void constructFingerTable() {
		fingerTable = new FingerTable(4, guid);
		fingerTable.constructFingerTable(liveNodes);
	}

	public void insertFileAtID(int id, String file) {
		if(id == guid) {
			System.out.println("File :" + file + "inserted at :" + guid);
			files.add(file);
		}else {
			boolean isNodeAvailable = fingerTable.successorTable.containsKey(id);
			if (isNodeAvailable) {
				int successorID = fingerTable.successorTable.get(id);
				transferFileToNode(successorID, successorID, file);
			}else {
				int sourceID;
				int minDistance = 20;
				TreeMap<Integer, Integer> nodesDistance = new TreeMap<Integer, Integer>();
				for(Map.Entry<Integer, Integer> entry: fingerTable.successorTable.entrySet()) {
					sourceID = entry.getKey();
					int distance = distanceBetweenNodes(sourceID, id);
					minDistance = Math.min(minDistance,distance);
					nodesDistance.put(distance, sourceID);
				}
				int idToSend = nodesDistance.get(minDistance);
				int successorId = fingerTable.successorTable.get(idToSend);
				if(checkBetweenNodes(idToSend, successorId, id)) {
					transferFileToNode(successorId, successorId, file);
				}else {
					transferFileToNode(id, successorId, file);
				}
				
			}
			
		}
		
	}
	
	public boolean checkBetweenNodes(int startNode, int endNode, int actualNode ) {
		int i= startNode;
		boolean isBetween = false;
		while(i!= endNode) {
			i = Math.abs( (startNode + 1) % 16 ) ;
			if(i == actualNode) {
				isBetween = true;
				break;
			}	
		}
		return isBetween;
	}
	
	public int distanceBetweenNodes(int sourceId, int destId ) {
		int distance = 1;
		int result;
		for(;;) {
			result = (distance + sourceId) % MAX_NODES;
			if(result == destId) {
				break;
			}
			distance++;
		}
		return distance;
	}
	
	public void transferFileToNode(int idToBeInserted, int successorID, String file) {
		try {
			InetAddress successorIP = liveNodes.get(successorID);
			Socket socket = new Socket(successorIP, LISTENING_PORT);
			ObjectOutputStream opStream = new ObjectOutputStream(socket.getOutputStream());
			opStream.writeObject("Insert File");
			System.out.println("File :" + file + " = " + idToBeInserted  + " routed to :" + successorID);
			opStream.writeObject(idToBeInserted);
			opStream.writeObject(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			ServerSocket listeningSocket = new ServerSocket(LISTENING_PORT);
			while (true) {
				Socket socket = listeningSocket.accept();
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				switch((String)inputStream.readObject()) {
				case	 "Finger Table":
					liveNodes = (TreeMap<Integer, InetAddress>) inputStream.readObject();
					System.out.println("Received updated livenodes");
					constructFingerTable();
					break;
				case "Insert File" :
					int id = (int)inputStream.readObject();
					String file = (String)inputStream.readObject();
					System.out.println("Insert file at :" + id);
					System.out.println("File: "+ file);
					insertFileAtID(id, file);
					break;
				case "Find File" :
					break;
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			
		}
		

	}

//	public void listenerForUpdatingFingerTable() {
//		this.isListeningThread = false;
//		while (true) {
//			try {
//				ServerSocket listeningSocket = new ServerSocket(LISTENING_PORT);
//				Socket socket = listeningSocket.accept();
//				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
//				// ObjectOutputStream outputStream = new
//				// ObjectOutputStream(socket.getOutputStream());
//				liveNodes = (TreeMap<Integer, InetAddress>) inputStream.readObject();
//				System.out.println("Received updated livenodes");
//				new Thread(this).start();
//			} catch (Exception e) {
//				// TODO: handle exception
//			}
//		}
//	}
}
