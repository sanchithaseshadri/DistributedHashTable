/**
 * SuperNode.java
 */

import java.io.*;
import java.util.*;
import java.lang.Math;
import java.math.BigInteger;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

import java.net.ConnectException;
import java.security.NoSuchAlgorithmException;
import java.net.UnknownHostException;

/**
 * Class representing the super node in the DHT that will keep track of
 * all the DHT nodes and their status (online or not), and the finger
 * tables of each of the DHT nodes.
 * 
 * @author	Sanchitha Seshadri
 *
 */
public class SuperNode {

	private static int DHT_SIZE = (int)Math.pow(2, 2);
	private static String SECRET_KEY = new String("firefly");

	private int PORT = 5000;
	private static int registeredNodes = 0;
	// mapping of each node to its status - true if online, false otherwise0
	private static Map<Integer, Boolean> nodeStatus = new HashMap<>();
	// mapping of node id to its ip address and port number
	private static Map<Integer, List<String>> ipMap = new HashMap<>();
	// mapping of node id to its finger table
	private static Map<Integer, List<List<Integer>>> fingerTables = new HashMap<>();


	/**
	 * Register all nodes part of the DHT in the beginning
	 */
	private void startDHT() {
		try {
			ServerSocket serverSocket = new ServerSocket(PORT);
			while (true) {
				Socket nodeSocket = serverSocket.accept();
				Thread nodeHandler = new Thread(new Runnable() {
					public void run() {
						/*if (registeredNodes == DHT_SIZE)
							checkStatus();*/
						handleInput(nodeSocket);
					}
				});
				nodeHandler.start();
			}
			
		} catch (IOException e) {
			System.out.println("Could not start server on port " + PORT);
		}
	}

	/**
  	 * Updates the status of a node to given status
  	 */
	private synchronized void nodeStatusUpdate(int id, boolean val) {
		this.nodeStatus.put(id, val);
	}


	/**
	 *  Updates the ip and port of a particular node
	 */
	private synchronized void ipMapUpdate(int id, List<String> address) {
		this.ipMap.put(id, address);
	}


	/**
	 * Registers Chord DHT node
	 */
	private synchronized int registerNode(String ip, String port) {
		// hash the ip and port to generate a node id
		String hash_text = new String(ip+port);
		int hash_value = 0;
		try {
			MessageDigest hash = MessageDigest.getInstance("SHA-1");
			hash.update(hash_text.getBytes());
			BigInteger big_hash = new BigInteger(1, hash.digest());
			BigInteger hash_mod = new BigInteger(Integer.toString(DHT_SIZE));
			// if another node's hash value is repeated, find closest available nodeID
			hash_value = big_hash.mod(hash_mod).intValue();
			while (this.ipMap.containsKey(hash_value)) {
				hash_value = (hash_value + 1) % DHT_SIZE;
			}
			List<String> node_address = new ArrayList<>();
			node_address.add(ip);
			node_address.add(port);
			this.ipMapUpdate(hash_value, node_address);
			this.nodeStatusUpdate(hash_value, true);
			this.registeredNodes += 1;
			// send this id back to the DHTNode
			return hash_value;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return hash_value;
	}

	/**
	 * Checks the status of each node in the Chord DHT
	 */
	private void checkStatus() {
		// if a node is offline, report it as offline and update finger tables
		boolean deltas = false;
		System.out.println("Checking status of nodes..");
		try {
			for (Integer nodeId : ipMap.keySet()) {
				List<String> nodeAddress = ipMap.get(nodeId);
				String ip = nodeAddress.get(0);
				int port = Integer.parseInt(nodeAddress.get(1));
				boolean status = this.isNodeOnline(ip, port);
				if (status != this.nodeStatus.get(nodeId)) {
					deltas = true;
					this.nodeStatusUpdate(nodeId, status);
					System.out.println("node " + nodeId + " status: " + status);
				}
			}
			// if changes detected, recompute all finger tables
			if (deltas)
				this.UpdateFingerTables();
		}
		 catch (ConcurrentModificationException e) {
			System.out.println("Status of some node(s) changed. ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Checks whether a node is online by trying to connect to it on the given ip and port
	 */
	private boolean isNodeOnline(String ip, int port) {
		try {
			Socket nodeSocket = new Socket(InetAddress.getByName(ip), port);
			nodeSocket.close();
			return true;
		} catch (ConnectException e) {
			System.out.println("Could not connect - node offline");
			return false;
		} catch (IOException e) {
			System.out.println("Could not connect - node offline");
			return false;
		}
	}

	/**
	 * Sends the appropriate socket for a given node
	 */
	private Socket getResponseSocket(int nodeId) {
		List<String> node_address = ipMap.get(nodeId);
		int port = Integer.parseInt(node_address.get(1));
		Socket nodeSocket = null;
		try {
			InetAddress ip = InetAddress.getByName(node_address.get(0));
			nodeSocket = new Socket(ip, port);
		} catch (ConnectException e) {
			System.out.println("Node " + nodeId + " not online.");
		} catch (UnknownHostException e) {
			System.out.println("Unknown node.");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return nodeSocket;
	}

	/**
	 * Sends a message to a particular node
	 */
	private void sendMessage(int nodeId, String message) {
		Socket nodeSocket = this.getResponseSocket(nodeId);
		try {
			PrintWriter out = new PrintWriter(nodeSocket.getOutputStream(), true);
			out.println(message);
			out.close();
			nodeSocket.close();
			System.out.println("Sent response to " + nodeId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			System.out.println("Node offline. Cannot send response.");
		}
	}

	/**
	 * Finds the next online successor of a given node
	 */
	private int nextOnlineSuccessor(int id) {
		// find next online successor of node id
		int key = (id + 1) % DHT_SIZE;
		while (key != id) {
			List<String> node_address = this.ipMap.get(id);
			String ip = node_address.get(0);
			int port = Integer.parseInt(node_address.get(1));
			if (this.isNodeOnline(ip, port))
				return key;
			key = (key + 1) % DHT_SIZE;
		}
		// no online node except id itself
		return -1;
	}

	/**
	 * Handles messages from other nodes and clients and calls appropriate subroutines
	 */
	private void handleInput(Socket nodeSocket) {
		System.out.println("Handling input from " + nodeSocket.getInetAddress());
		try (
			BufferedReader in = new BufferedReader(
				new InputStreamReader(nodeSocket.getInputStream()));
		) {
			String message = in.readLine();
			String[] messageChunked = message.split("\\s+");
			System.out.println("message from node : " + message);
			// invalid DHTnode - doesn't have the required secret key
			if (!messageChunked[0].equals(SECRET_KEY)) 
				return;

			switch(messageChunked[1]) {
				case "register": {	// firefly register <port>
					if (this.registeredNodes == DHT_SIZE)	// extra nodes not allowed
						break;
					InetAddress ip = nodeSocket.getInetAddress();
					String port = messageChunked[2];
					int id = this.registerNode(ip.getHostAddress(), port);
					String msg = SECRET_KEY + " id " + id;
					this.sendMessage(id, msg);
					this.checkStatus();
					break;
				}
				case "online": {	// firefly online <nodeID> <port>
					// update its ip and port in ipMap, and status in nodeStatus
					InetAddress ip = nodeSocket.getInetAddress();
					int id = Integer.parseInt(messageChunked[2]);
					String port = messageChunked[3];
					List<String> node_address = new ArrayList<>();
					node_address.add(ip.getHostAddress());
					node_address.add(port);
					this.ipMapUpdate(id, node_address);
					this.nodeStatusUpdate(id, true);
					System.out.println("node back online - retrieving data..");
					if (this.registeredNodes == DHT_SIZE) {
					// find next online successor of current node
						int source = this.nextOnlineSuccessor(id);
						if (source == -1) {
							System.out.println("All other nodes offline - unable to retrieve data at the moment");
							break;
						}
						// retrieve data belonging to the current node
						String msg = SECRET_KEY + " rehash " + id;
						this.sendMessage(source, msg);
					}
					this.checkStatus();
					break;
				}
				case "store": {		// firefly store <data> <flag>
					if (this.registeredNodes != DHT_SIZE) {
						System.out.println("Waiting for Chord to start up. Try again.");
						break;
					}
					int data = Integer.parseInt(messageChunked[2]);
					String flag = messageChunked[3];
					if (flag.equals("false"))
						this.checkStatus();
					this.routeData(data, flag);
					break;
				}
				case "off": {		// firefly off <nodeID>
					if (this.registeredNodes < DHT_SIZE) {
						System.out.println("Cannot go offline now - initializing Chord.");
						break;
					}
					int off = Integer.parseInt(messageChunked[2]);
					if ((off < 0) || (off > DHT_SIZE-1)) {
						System.out.println("Invalid node supplied");
						break;
					}
					this.nodeStatusUpdate(off, false);
					this.UpdateFingerTables();
					// send a move message to node
					String msg = SECRET_KEY + " off ";
					this.sendMessage(off, msg);
					break;
				}
				default: {
					break;
				}
			}
			
			nodeSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Routes given data to the right node using finger tables
	 */
	private synchronized void routeData(int data, String flag) {
		// send data to appropriate node in the DHT
		int key = data % DHT_SIZE;	// ideal location of key
		int destination = -1;
		// ideal case - place data at key if nodeID = key is online
		if (this.nodeStatus.get(key)) {
			destination = key;
		} else {	//must route to nodeID closest to key
			List<List<Integer>> table = this.fingerTables.get(key);
			List<Integer> entry = table.get(0);	// get successor of closest node
			destination = entry.get(1);	// closest online node to key
		}
		String msg = SECRET_KEY + " store " + data + " " + flag;
		this.sendMessage(destination, msg);
		System.out.println("routed data " + data + " to node " + destination);
	}

	/**
	 * Updates finger tables of all nodes in the Chord DHT
	 */
	private synchronized void UpdateFingerTables() {
		System.out.println("Updating finger tables");
		if (this.registeredNodes < DHT_SIZE)
			return;
		// update finger tables of all nodes
		int tableSize = (int) Math.pow(DHT_SIZE, 0.5);	// entries in each finger table
		for (int k=0; k<DHT_SIZE; k++) {
			List<List<Integer>> table = new ArrayList<>();
			for (int i=0; i<tableSize; i++) {
				List<Integer> entry = new ArrayList<>();
				int inode = ((int) Math.pow(2, i) + k) % DHT_SIZE;
				int successor = -1;
				if (this.nodeStatus.get(inode))
					successor = inode;
				else {
					int temp = (inode + 1) % DHT_SIZE;
					while(temp != inode) {
						if (this.nodeStatus.get(temp)) {
							successor = temp;
							break;
						}
						temp = (temp + 1) % DHT_SIZE;
					}
				}
				entry.add(inode);
				entry.add(successor);
				table.add(entry);
			}
			this.fingerTables.put(k, table); 
			// uncomment below to see finger table of each node
			System.out.println("Finger table of node " + k);
			for(int c=0; c<tableSize; c++)
				System.out.println(table.get(c));
		}
	} 

	/**
	 * Main method of the SuperNode class - starts the DHT
	 */
	public static void main(String[] args) throws UnknownHostException, 
												  NoSuchAlgorithmException {
		new SuperNode().startDHT();
	}

}