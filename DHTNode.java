/**
 * DHTNode.java
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

import java.io.FileNotFoundException;
import java.net.ConnectException;
import java.security.NoSuchAlgorithmException;
import java.net.UnknownHostException;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Class representing each node in the Chord DHT circular space
 *
 * @author 	Sanchitha Seshadri
 *
 */
class DHTNode {

	private static String SUPERNODE_ADDRESS = "172.17.0.2";
	private static int SUPERNODE_PORT = 5000;
	private static String ID_FILE = "ChordNodeID";
	private static String SECRET_KEY = "firefly";

	private int listenPort;
	private int nodeId;
	private DHTNodeListener nodeListener;

	ReentrantLock consoleLock = new ReentrantLock(true);
    Object registerLock = new Object();

    /**
     * Constructor - spins up the Chord DHT node - registers if node hasn't been registered before,
     * retrieves port and node id if coming back online
     */
	public DHTNode() throws IOException {
		this.checkIdFile();

		// select port to use
		if (this.listenPort == 0) {
			try (ServerSocket freeSocket = new ServerSocket(0)) {
				this.listenPort = freeSocket.getLocalPort();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		this.nodeListener = new DHTNodeListener(this.listenPort, ID_FILE, consoleLock, registerLock);
		new Thread(nodeListener).start();

		if (this.nodeId == -1) {
			// node has not been registered with the SuperNode of the DHT yet
			this.register();
			System.out.println("registering DHT node..");
			synchronized (registerLock) {
				try {
					registerLock.wait();				
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			System.out.println("node registered");
			} 
		} else {
			System.out.println("Node back online. Updating SuperNode..");
			// read node id file to update values of ip and port
			this.checkIdFile();
			this.updateSuperNode();
		}
	}

	/**
	 * Sends message to the SuperNode
	 */
	public void sendMessage(String message) {
		try {
			Socket socket = new Socket(SUPERNODE_ADDRESS, SUPERNODE_PORT);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(message);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Register the Chord DHT node with Chord DHT SuperNode
	 */
	public void register() {
		// register DHT node with DHT SuperNode for the first time
		this.sendMessage(SECRET_KEY + " register " + this.listenPort);
	}

	/**
	 * If node coming back online, update the Chord DHT SuperNode of its new ip and port
	 */	
	public void updateSuperNode() throws IOException, UnknownHostException {
		String message = SECRET_KEY + " online " + this.nodeId + " " + this.listenPort;
		this.sendMessage(message);
	}

	/**
	 * Checks the id file to get the node id and port to listen on
	 */
	public void checkIdFile() {
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader(ID_FILE));
			this.nodeId = Integer.parseInt(fileReader.readLine());
			this.listenPort = Integer.parseInt(fileReader.readLine());
			fileReader.close();
		} catch (FileNotFoundException e) {
			this.nodeId = -1;
			this.listenPort = 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Main method of the Chord DHT - starts up the DHT node
	 */
	public static void main(String[] args) throws IOException {
		new DHTNode();
	}

}
