import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.InputMismatchException;
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

	public Peer(int guid, String host) {
		// TODO Auto-generated constructor stub
		this.guid = guid;
		this.host = host;
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
			System.out.println("1. Join the network \n 2. Leave the network\n 3. Get file\n 4. Show finger table");
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

	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (isListeningThread) {
			listenerForUpdatingFingerTable();
		} else {
			constructFingerTable();
		}

	}

	public void listenerForUpdatingFingerTable() {
		this.isListeningThread = false;
		while (true) {
			try {
				ServerSocket listeningSocket = new ServerSocket(LISTENING_PORT);
				Socket socket = listeningSocket.accept();
				ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
				// ObjectOutputStream outputStream = new
				// ObjectOutputStream(socket.getOutputStream());
				liveNodes = (TreeMap<Integer, InetAddress>) inputStream.readObject();
				System.out.println("Received updated livenodes");
				new Thread(this).start();
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
}
