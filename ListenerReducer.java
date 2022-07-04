import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.ObjectInputStream;
import java.io.PipedOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.io.FileOutputStream;
import java.io.IOException;

public class ListenerReducer extends Thread {
/**  Listens to an ObjectInputStream for the list of all machines, for splits, and for words.
    Called as a new Thread in SimpleServerProgram.java to run concurrently with WorkerSender.java.
    Each server therefore has a thread for listening to incoming data and another for 
	running computations on a split, that run concurrently.

    When the list of all machines is received, it is saved in the volatile variable to be used by WorkerSender.
    When a split is received, the split is saved locally on the machine as a .txt file to be used by WorkerSender.
    When a word is received (shuffling step), the volatile HashMap with received words as keys is incremented by one
    for the corresponding word.
*/
	private static int port = 12302;

    // The list of all machines.
	private final MachineList machines;

	// Total number of machines to wait for.
	private int numberOfMachines = 0;

	// Count the number of machines that have finished.
	private static int finishedMachines = 0;


	// Input pipe to send information to WorkerSender thread.
	private final PipedOutputStream pos;

	// Queue to send strings from ListenerReducer to WorkerSender threads.
	private final ConcurrentLinkedQueue<String> splitQueue;
	

	// Client socket channel.
	private SocketChannel clientSocket;

	// Client string address
	private String clientAddr;

	// Selectable channel to bind sockets to
	private static ServerSocketChannel listenerServerSocket = null;

	// Selector to handle multiple sockets (different ports)
	private static Selector listenerSelector = null;

	// Buffer size
	private static final int BUFFER_SIZE = 128 * 1024;


	// Words that belong to this machine (shuffle step) and their count (reduce step).
	private final HashMap<String, Integer> myWords;

	// Keep Hashmaps of buffers for each client
	private static HashMap<SocketChannel, ByteBuffer> buffers = new HashMap<SocketChannel, ByteBuffer>();


	// To end the thread from dealWithReceivedObject
	private static boolean allMachinesFinished = false;

    ListenerReducer(MachineList machines,
					HashMap<String, Integer> myWords,
					PipedOutputStream pos,
					SocketChannel clientSocket,
					ConcurrentLinkedQueue<String> splitQueue) {
        this.machines = machines;
		this.myWords = myWords;
		this.pos = pos;
		this.clientSocket = clientSocket;
		this.splitQueue = splitQueue;
    }

	public void receivedSplit(Split split) {
		/** When a split is received, the split is saved locally on the machine
		 *  as a .txt file to be used by WorkerSender.
		 */
		
		BufferedOutputStream bos = null;

		try {

			if (clientAddr != null) {
				clientAddr = split.getSender();
			}

			if (split.getStatus().compareTo("Success") == 0) {
				// Save the split and communicate the reception to the WorkerSender thread.
				String outputFile = split.getDestinationDirectory() + split.getFileName();
				File dstFile = new File(outputFile);
				bos = new BufferedOutputStream(new FileOutputStream(dstFile));
				bos.write(split.getFileData());

				bos.flush();
				bos.close();
				System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Split received : " + outputFile + " is successfully saved");

				splitQueue.add(outputFile);
			
			} else if (split.getStatus().compareTo("Error") == 0) {
				System.out.println("LR: Error on file " + split.getFileName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void receivedWord(String wordString) {
		/** If users send a Word (Shuffle step): increment the count of the word in the HashMap.
			If the word is not in the HashMap, add it with count 1.
		 */

		try {
			// System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Word received : " + wordString); 
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (myWords.containsKey(wordString)) {
			myWords.put(wordString, myWords.get(wordString)+1);
		} else {
			myWords.put(wordString, 1);
		}
	}

	public void receivedMachines(MachineList receivedMachines) {
		/** If server sends a MachineList : save the list of machines.
		 *  This list is used by WorkerSender to send the list of machines to the clients.
		 * 	The list of machines is used to know to which machines WorkerSender should open a connection.
		 */

		try {
			System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: List of all machines received");
		} catch (Exception e) {
			e.printStackTrace();
		}	
		// If client sent array of machines : save the machines in the volatile variable,
		// and communicate the reception to the WorkerSender thread.
		this.machines.setMachines(receivedMachines.getMachines());
		numberOfMachines = this.machines.getMachines().size();

		// Communicate reception to WorkerSender to open sockets.
		try {
			pos.write(1); // 1 for machine
			pos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void receiviedFinishedSplit() {
		finishedMachines++;
		if (numberOfMachines > 0 && finishedMachines == numberOfMachines) {
			try {
				// Tell WorkerSender to send counted words back to the client
				pos.write(4); // 4 when all machines have finished
				pos.flush();
				// Close all sockets
				listenerServerSocket.close();
				// Flag to end the thread
				allMachinesFinished = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
    
    public void run() {
	
		// To received objects from the input stream.
		Object receivedObject = null;

		int numSplits = 0;

		try {
			// Open selector and selectable sockets and configure them properly
			listenerSelector = Selector.open();

			// Create a server socket channel and bind it to the port
			listenerServerSocket = ServerSocketChannel.open();
			listenerServerSocket.configureBlocking(false);
			String myName = InetAddress.getLocalHost().getCanonicalHostName();

			InetSocketAddress listenerAddress = new InetSocketAddress(myName, port);
			listenerServerSocket.bind(listenerAddress);
			System.out.println("LR: Bound machine " + myName + " on port " + port);

			// Register the server socket channel with the selector
			listenerServerSocket.register(listenerSelector, SelectionKey.OP_ACCEPT);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (true) {
			try {
				listenerSelector.select(); // Blocks until at least one channel is ready.
				Set<SelectionKey> selectedKeys = listenerSelector.selectedKeys();
				Iterator<SelectionKey> iter = selectedKeys.iterator();
				while (iter.hasNext()) {
					// Deal with all connections that are ready at select time.
					SelectionKey key = iter.next();
					
					if (key.isAcceptable()) {
						// Accept a new connection.
						SocketChannel client = listenerServerSocket.accept();
						client.configureBlocking(false);
						client.register(listenerSelector, SelectionKey.OP_READ);
						System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Connection accepted from client: " + client.getRemoteAddress());

						buffers.put(client, ByteBuffer.allocate(BUFFER_SIZE));
						
					} else if (key.isReadable()) {
						// Read object from ByteBuffer
						SocketChannel currentClient = (SocketChannel) key.channel();
						
						// Read data from the client and put it in the buffer
						// First clear the buffer, then fill it with data.
						ByteBuffer buffer = buffers.get(currentClient);
						buffer.clear();

						// System.out.println("Received buffer" + buffer.toString());
						currentClient.read(buffer); 
						// System.out.println("Read buffer " + buffer.toString());

						if (clientSocket == null) {
							// If the client socket is null, save the client socket.
							clientSocket = currentClient;

							System.out.println(clientSocket.toString());
						}
						
						// If the incoming data comes from the client : it is an object
						if (currentClient == clientSocket) {
							// Object input stream to get objects back
							ObjectInputStream byteOis = null;
							try {
								if (buffer.position() == 0) {
									// If the buffer is empty, we skip this iteration
									// This should not happen but can sometimes happen and can block the execution if not dealt with
									// System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Empty buffer received");
									iter.remove();
									continue;
								} else if (buffer != null) {
									byteOis = new ObjectInputStream(new ByteArrayInputStream(buffer.array()));
									// System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " Reading " + buffer.toString()+ ", available " + byteOis.available());
									receivedObject = byteOis.readObject();
									// System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " Read " + buffer.toString());

									// Deal with received object
									if (receivedObject instanceof MachineList) {
										// Save list of machines
										receivedMachines((MachineList) receivedObject);
									} else if (receivedObject instanceof Split) {
										// Save split
										receivedSplit((Split)receivedObject);
										numSplits++;
									} else if (receivedObject instanceof SplitCount) {
										for (int i = 0; i<numSplits; i++) {
											pos.write(2); // 2 to indicate a new split is available to WorkerSender.
											pos.flush();
										}
										try {
											pos.write(3); // send 3 to WS when all splits are received
											pos.flush();
										} catch (IOException e) {
											e.printStackTrace();
										}
									}
								}
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							} catch (StreamCorruptedException e) {
								System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Corrupted stream");
								e.printStackTrace();
								iter.remove();
								return;
							} finally {
								if (byteOis != null) {
									byteOis.close();
								}
							}
						} else if (currentClient != clientSocket) {
							// If the incoming data comes from a server machine, it is a string
							// Read the string and perform the corresponding action
							// System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Received string " + buffer.toString());

							try {
								String wholeString = new String(buffer.array(), 0, buffer.position(), "UTF-8");
								String [] splitString = wholeString.split("\n");

								for (String s : splitString) {
									if (s.equals("$FINISHED_SPLITS$")) {
										System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Received string: " + s);
										receiviedFinishedSplit();
										if (allMachinesFinished) {
											// If the flag to end is true, then we exit the thread
											// The flag is set to true when all machines have finished
											System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: All machines have finished, ending thread");
											return;
										}
									} else if (allMachinesFinished && s.isBlank()) {
										// Safety net to end thread if all machines have finished and the last line is empty
										// Avoids falling into infinite loop
										System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: All machines have finished, ending thread");
										return;
									} else if (!s.isBlank()) {
										// Increment word count
										receivedWord(s);
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					// Remove element from the iterator to get the next element
					iter.remove();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
