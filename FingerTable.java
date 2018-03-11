import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.net.InetAddress;

public class FingerTable {
	public int noOfEntries;
	public TreeMap<Integer, Integer> successorTable;
	public int nodeID;
	public static final int MAX_NODES = 16;
	
	public FingerTable(int entries, int id) {
		nodeID = id;
		noOfEntries = entries;
		successorTable = new TreeMap<Integer, Integer>();		
	}
	
	public void constructFingerTable(TreeMap<Integer, InetAddress> liveNodes) {
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
	
	public void printFingerTable() {
		Set<Entry<Integer, Integer>> set = successorTable.entrySet();
		Iterator<Entry<Integer, Integer>> iter = set.iterator();
		System.out.println("Actual \t" + "Successor");
		while (iter.hasNext()) {
			Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>) iter.next();
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}
	
	
}