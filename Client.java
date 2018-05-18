/**
 * Client.java
 */
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Class representing any client using the Chord DHT - handles requests to hash data
 * 
 * @author	Sanchitha Seshadri
 *
 */
public class Client {
	// address of the supernode - change based on ip
	private static String DHT_ADDRESS = "172.17.0.2";
	private static int DHT_PORT = 5000;
	private static String SECRET_KEY = "firefly";

	private int listenPort;
	private BufferedReader stdIn;

	/**
	 * Constructor of the Client class - starts the client and initializes port to listen to
	 */ 
	public Client() {
		try (ServerSocket freeSocket = new ServerSocket(0)) {
			this.listenPort = freeSocket.getLocalPort();
		} catch (IOException e) {
			System.out.println("Could not start client.");
			e.printStackTrace();
		}
	}

	/**
	 * Hash data into the Chord DHT
	 */
	protected void hashData(int data, String flag) {
		String message = SECRET_KEY + " store " + data + " " + flag; 
		this.sendMessage(message);
	}

	/**
	 * Begin hashing the data into the DHT
	 */
	private void startHashing() {
		this.stdIn = new BufferedReader(new InputStreamReader(System.in));

		System.out.println("Enter data to store: ");
		int data = 0;
		try {
			data = Integer.parseInt(stdIn.readLine());
			// contact SuperNode to hash data
			this.hashData(data, "false");
			System.out.println("DONE HASHDATA");
		} catch (NumberFormatException e) {
			System.out.println("Invalid data format. Only integer supported.");
		} catch (IOException e) {
			System.out.println("Something went wrong in client input. ");
			e.printStackTrace();
		}
	}

	/**
	 * Tell a particular node to go offline
	 */
	private void offline() {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter node to make offline: ");
		try {
			int node = Integer.parseInt(in.readLine());
			String msg = SECRET_KEY + " off " + node;
			this.sendMessage(msg);
		} catch (NumberFormatException e) {
			System.out.println("Invalid input");
		} catch (IOException e) {
			System.out.println("IOException occurred");
		}
	}

	/**
	 * Show the client the menu and choose subroutine based on it
	 */
	private void menu() {
		int choice;
		BufferedReader menuIn = new BufferedReader(new InputStreamReader(System.in));
		while(true) {
			System.out.println("1. Enter data to store");
			System.out.println("2. Make node offline");
			System.out.println("Enter selection: ");
			try {
				choice = Integer.parseInt(menuIn.readLine());

				switch(choice) {
					case 1:  {
						this.startHashing();
						break;
					}
					case 2: {
						this.offline();
						break;
					}
					default: {
						System.out.println("Invalid choice - try again.");
					}
				}

			} catch (NumberFormatException e) {
				System.out.println("Invalid input. ");
			} catch (IOException e) {
				System.out.println("IO Exception occurred");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Send message to the Chord DHT
	 */
	private void sendMessage(String message) {
		try (
			Socket socket = new Socket(DHT_ADDRESS, DHT_PORT);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			) {
			out.println(message);
		} catch (IOException e) {
			System.out.println("Something went wrong while sending data to SuperNode");
			e.printStackTrace();
		}
		System.out.println("sent data hash request to the SuperNode");
	}

	/**
	 * Main method of the Client class - starts the client menu
	 */
	public static void main(String[] args) {
		new Client().menu();
	}

}