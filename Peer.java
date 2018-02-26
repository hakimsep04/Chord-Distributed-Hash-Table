import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.TreeMap;


public class Peer {
	public static void main(String[] args) throws ClassNotFoundException {
		try {
			if (args.length == 0) {
				System.out.println("Please provide host");
				System.exit(0);
			}
			String host = args[0];
			InetAddress serverIP = InetAddress.getByName(host);
			Socket peer = new Socket(serverIP, 5000);
			ObjectInputStream inputstream = new ObjectInputStream(peer.getInputStream());
			System.out.println("Attempt");
			ObjectOutputStream outputStream = new ObjectOutputStream(peer.getOutputStream());
			int guid = (int) inputstream.readObject();
			TreeMap<Integer, InetAddress> liveNodes = (TreeMap<Integer, InetAddress>) inputstream.readObject();
			System.out.println(guid);
			System.out.println("Size : " + liveNodes.size());
			outputStream.writeObject("Received GUID");
			
			FingerTable fingerTable = new FingerTable(4, guid);
			fingerTable.newFingerTable(liveNodes);
			
		}catch (IOException e) {
			// TODO: handle exception
		}
		
	}
}
