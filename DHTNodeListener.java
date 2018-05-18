/**
 * DHTNodeListener.java
 */

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Listener thread for each DHT node that listens for responses from the SuperNode
 */
class DHTNodeListener implements Runnable {

	private int port;
	private String idFile;
	protected Set<Integer> nodeData = new HashSet<>();
	ReentrantLock consoleLock;
    Object registerLock;

	private static int DHT_SIZE = (int)Math.pow(2, 2);
	private static String SECRET_KEY = "firefly";

	/**
	 * Constructor - initializes listener thread of Chord DHT node
	 */
	public DHTNodeListener(int port, String idFile, ReentrantLock consoleLock, Object registerLock) {
		this.port = port;
		this.idFile = idFile;
		this.consoleLock = consoleLock;
        this.registerLock = registerLock;
	}

	/**
	 * Run method defined for thread - constantly listens to the SuperNode for 
	 * store/move/rehash instructions
	 */
	public void run() {
		try (ServerSocket listener = new ServerSocket(port)) {
			consoleLock.lock();
			System.out.println("Listening on port " + port + "...");
			consoleLock.unlock();
			while(true) {
				Socket listenerSocket = listener.accept();
				consoleLock.lock();
				BufferedReader in = new BufferedReader(
					new InputStreamReader(listenerSocket.getInputStream()));
				String message = in.readLine();
				// if not just a ping to check status
				if (message != null)
					this.handleInput(message);
				listenerSocket.close();
				consoleLock.unlock();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method to write id and port into file upon registeration
	 */
	public void writeIdFile(String id) {
		try (BufferedWriter fileWriter = new BufferedWriter(
			new FileWriter(this.idFile))) {
			fileWriter.write(id);
			fileWriter.newLine();
			fileWriter.write(this.port + "");
		} catch (IOException e) {
			e.printStackTrace();
		}
		synchronized (registerLock) {
        	registerLock.notify();
    	}
	}

	/**
	 * Rehash values of a certain key
	 */
	public synchronized void rehash(int key) {
		// remove all data where data % key is 0 and rehash those
		Set<Integer> delete = new HashSet<>();
		System.out.println("Moving data with hash mod " + key);
		Iterator<Integer> it = this.nodeData.iterator();
		while (it.hasNext()) {
			Integer value = it.next();
			if (value % DHT_SIZE == key) {
				delete.add(value);
				new Client().hashData(value, "false");
				System.out.println("moved " + value);
			}
		}
		// remove the rehashed data from this node
		Iterator<Integer> it_delete = delete.iterator();
		while (it_delete.hasNext()) {
			Integer data = it_delete.next();
			this.nodeData.remove(data);
		}
	}

	/**
	 * Move all data to a successor node since this one is going offline
	 */
	public synchronized void offline() {
		// move all data to successor before going offline
		System.out.println("shutting down - transferring data to successor..");
		Iterator<Integer> it = this.nodeData.iterator();
		while (it.hasNext()) {
			Integer value = it.next();
				//this.nodeData.remove(value);
				new Client().hashData(value, "true");
		}
		this.nodeData.clear();
	}

	/**
	 * Handle all instructions from SuperNode of Chord DHT
	 */
	public synchronized void handleInput(String message) {
		System.out.println("message: " + message);
		String[] messageChunks = message.split("\\s+");
		if (!messageChunks[0].equals(SECRET_KEY))
			return;
		switch(messageChunks[1]) {
			case "id" :		{ 	// firefly id <nodeID> 
				String id = messageChunks[2];
				this.writeIdFile(id);
				System.out.println("Registered with Chord SuperNode");
				break;
			}
			case "store" : {	// firefly store <data>
				int data = Integer.parseInt(messageChunks[2]);
				this.nodeData.add(data);
				System.out.println("Stored " + data);
				System.out.println("Node data: " + this.nodeData);
				break;
			}
			case "rehash" : {	// firefly rehash <key>
				int key = Integer.parseInt(messageChunks[2]);
				this.rehash(key);
				System.out.println("Moved data belonging to node " + key);
				System.out.println("Node data: " + this.nodeData);
				break;
			}
			case "off" : {	// firefly off
				// move all existing data
				this.offline();
				System.out.println("All data moved. Shutting down..");
				System.exit(0);
				break;
			}
			default: {
				break;
			}
		
		}
	}

}