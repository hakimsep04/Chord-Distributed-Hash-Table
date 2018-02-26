import java.util.HashMap;
import java.util.TreeMap;
import java.net.InetAddress;

public class FingerTable {
	public int noOfEntries;
	public HashMap<Integer, Integer> successorTable;
	public int nodeID;
	public static final int MAX_NODES = 16;
	
	public FingerTable(int entries, int id) {
		nodeID = id;
		noOfEntries = entries;
		successorTable = new HashMap<Integer, Integer>();		
	}
	
	public void newFingerTable(TreeMap<Integer, InetAddress> liveNodes) {
		for (int i = 0; i<noOfEntries; i++) {
			Object nodes[] = liveNodes.keySet().toArray();
			int successor = 0;
			boolean isSuccessorFound = false;
			int actualNode = ( nodeID + (int) Math.pow(2, i) ) % MAX_NODES;
			
			if (!liveNodes.containsKey(actualNode)) {
				for (int j = 0; j < nodes.length; j++) {
					int node = (int)nodes[j];
					if (node > actualNode) {
						successor = node;
						isSuccessorFound = true;
						break;
					}
				}
				if (!isSuccessorFound) {
					successor = (int)nodes[0];
				}
			}else {
				successor = actualNode;
			}
			successorTable.put(actualNode, successor);
			System.out.println(actualNode + " = " + successor);
		}
	}	
	
}
