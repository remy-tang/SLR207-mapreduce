import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.ObjectInputStream;
import java.io.PipedOutputStream;
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

	// Words that belong to this machine (shuffle step) and their count (reduce step).
	private final HashMap<String, Integer> myWords;

	// Input pipe to send information to WorkerSender thread.
	private final PipedOutputStream pos;

	// Address of the client to be passed to WorkerSender as volatile variable.
	private String clientAddr;

	// Count the number of machines that have finished.
	private int numberOfMachines;

	// Queue to send strings from ListenerReducer to WorkerSender threads.
	private final ConcurrentLinkedQueue<String> splitQueue;

    ListenerReducer(MachineList machines,
					HashMap<String, Integer> myWords,
					PipedOutputStream pos,
					String client,
					ConcurrentLinkedQueue<String> splitQueue) {
        this.machines = machines;
		this.myWords = myWords;
		this.pos = pos;
		this.clientAddr = client;
		this.splitQueue = splitQueue;
    }

	public void receivedSplit(Split split) {
		/** When a split is received, the split is saved locally on the machine as a .txt file to be used by WorkerSender.
		 */
		
		BufferedOutputStream bos = null;

		try {
			if (clientAddr.isEmpty()) {
				// Save the client address
				clientAddr = split.getSender();
				System.out.println("Saved client address: " + clientAddr);
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

				// Communicate reception of a new split to WorkerSender.
				splitQueue.add(outputFile);
				pos.write(2); // 2 to indicate a new split is available to WorkerSender.
				pos.flush();
				
				// splitsReceived++; // count the number of received splits
			
			} else if (split.getStatus().compareTo("Error") == 0) {
				System.out.println("LR: Error on file " + split.getFileName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void receivedWord(Word word) {
		/** If users send a Word (Shuffle step): increment the count of the word in the HashMap.
			If the word is not in the HashMap, add it with count 1.
		 */

		// try {
		// 	System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Word received : " + word.getWord()); 
		// } catch (Exception e) {
		// 	e.printStackTrace();
		// }

		String wordString = word.getWord();
		
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
    
    public void run() {
	
		// To received objects from the input stream.
		Object receivedObject = null;

		// Count the number of machines that have finished.
		int finishedMachines = 0;

		// Selector to handle multiple sockets (different ports)
		Selector listenerSelector = null;

		// Selectable channel to bind sockets to
		ServerSocketChannel listenerServerSocket = null;

		try {
			// Open selector and selectable sockets and configure them properly
			listenerSelector = Selector.open();

			// Create a server socket channel and bind it to the port
			listenerServerSocket = ServerSocketChannel.open();
			listenerServerSocket.configureBlocking(false);
			String myName = InetAddress.getLocalHost().getCanonicalHostName();
			// int port = Integer.parseInt("123".concat(myName.substring(9, 11)));
			InetSocketAddress listenerAddress = new InetSocketAddress(myName, port);
			listenerServerSocket.bind(listenerAddress);
			System.out.println("LR: Bound machine " + myName + " on port " + port);

			// Register the server socket channel with the selector
			listenerServerSocket.register(listenerSelector, SelectionKey.OP_ACCEPT);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Keep input streams and buffers for each socket
		HashMap<SocketChannel, ObjectInputStream> inputStreams = new HashMap<SocketChannel, ObjectInputStream>();
		HashMap<SocketChannel, ByteBuffer> buffers = new HashMap<SocketChannel, ByteBuffer>();

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

						// Buffer to read data (objects)
						ByteBuffer tempBuf = ByteBuffer.allocate(128 * 1024);
						// Open object stream to get objects back
						ObjectInputStream tempOis = new ObjectInputStream(new ByteArrayInputStream(tempBuf.array()));

						inputStreams.put(client, tempOis);
						buffers.put(client, tempBuf);
					} else if (key.isReadable()) {
						// Read object from ByteBuffer
						SocketChannel currentClient = (SocketChannel) key.channel();

						ObjectInputStream byteOos = inputStreams.get(currentClient);
						ByteBuffer buffer = buffers.get(currentClient);

						// Read data from the client and put it in the buffer.
						currentClient.read(buffer); 
						byteOos = inputStreams.get(currentClient);

						try {
							if (buffer.position() == 0) {
								// If the buffer is empty, we skip this iteration.
								iter.remove();
								continue;
							} else if (buffer != null) {
								receivedObject = byteOos.readObject();
							}
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (Exception e) {
							System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Error when creating ObjectInputStream");
							e.printStackTrace();
							iter.remove();
							continue;
						} finally {
							byteOos.close();
						}
						
						// Deal with received object
						if (receivedObject instanceof MachineList) {
							// Save list of machines
							receivedMachines((MachineList) receivedObject);

						} else if (receivedObject instanceof Split) {
							// Save split
							receivedSplit((Split)receivedObject);

						} else if (receivedObject instanceof Word) {
							// Add word to the local list of words or increment the counter.
							receivedWord((Word)receivedObject);
						} else if (receivedObject instanceof SplitCount) {
							pos.write(3); // 3 when all splits are received
							pos.flush();
						} else if (receivedObject instanceof FinishedMachine) {
							finishedMachines++;
							if (numberOfMachines > 0 && finishedMachines == numberOfMachines) {
								// Close all input streams
								for (ObjectInputStream ois : inputStreams.values()) {
									ois.close();
								}

								// Tell WorkerSender to send counted words back to the client
								pos.write(4); // 4 when all machines have finished
								pos.flush();
								// Close all sockets
								listenerServerSocket.close();
								// Exit the thread
								System.out.println("LR: All machines have finished");
								return;
							}
						}
						buffer.clear();
					}
					// Remove element from the iterator to get the next element.
					iter.remove();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
