import java.io.*;
import java.util.ArrayList;
import java.net.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
// import java.util.Collections;
// import java.util.Comparator;
// import java.util.HashMap;
// import java.util.Iterator;
// import java.util.List;
// import java.util.Map;
// import java.util.Set;
// import java.nio.CharBuffer;
// import java.nio.channels.FileChannel;
// import java.nio.channels.SelectionKey;
// import java.nio.channels.Selector;
// import java.nio.channels.ServerSocketChannel;

public class SimpleClient {

	private static String addr = "137.194.126.71";	// Client machine address (the machine that executes this program).
	private static int port = 12302;

	// private static int port_words = 12321; // This port should be open on the client machine

	// List of all machines participating
	// private static String machineNames = "tp-t310-13.enst.fr";
	// private static String machineNames = "tp-t310-13.enst.fr tp-t310-14.enst.fr";
	// private static String machineNames = "tp-t310-13.enst.fr tp-t310-14.enst.fr tp-t310-16.enst.fr";
	// private static String machineNames = "tp-t310-13.enst.fr tp-t310-14.enst.fr tp-t310-16.enst.fr tp-1a207-11.enst.fr";
	private static String machineNames = "tp-t310-13.enst.fr tp-t310-14.enst.fr tp-t310-16.enst.fr tp-1a207-11.enst.fr tp-3b01-02.enst.fr";

	private static String[] machines;

	private static String splitsSourceDirectory = "./splits/";

	private static String splitsDestinationDirectory = "/tmp/retang/splits/";

	private static String originalFileName = "./large_txt/lorem.txt";

	private static int staticSplitSize = 1; // splitSize in KB

	// private static int CLIENT_BUFFER_SIZE = 1024;

	// private static int NUM_WORDS_TO_PRINT = 50;

	// private static HashMap<SocketChannel, ByteBuffer> buffers = new HashMap<SocketChannel, ByteBuffer>();

	// private static HashMap<String, Integer> words = new HashMap<String, Integer>();

	public static void sendObject(SocketChannel client, Object object) {

        ByteArrayOutputStream baos = null;
        ObjectOutputStream oosSendObject = null;
		ByteBuffer buffer = null;

        try {

			// Write object to the ByteArrayOutputStream
            baos = new ByteArrayOutputStream();
            oosSendObject = new ObjectOutputStream(baos);
            oosSendObject.writeObject(object);
			oosSendObject.flush();

			try {
                Thread.sleep(80);
            } catch (InterruptedException e) {
                e.printStackTrace();
			}

			// Write the ByteArrayOutputStream to the SocketChannel
            buffer = ByteBuffer.wrap(baos.toByteArray());
            client.write(buffer);

			oosSendObject.close();
			baos.close();
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host " + object);
			e.printStackTrace();
			return;
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to " + object);
			e.printStackTrace();
			return;
		}
	}
    

	public static void cleanDirectory(String directory) {
		File dir = new File(directory);
		File[] files = dir.listFiles();
		if (files != null) {
			for (File f : files) {
				f.delete();
			}
		}
	}

	public static void makeSplitsFromFile(String fileName) {
		// Makes splits from a file.
		// The splits are saved in splitsSourceDirectory/fileName.split0, splitsSourceDirectory/fileName.split1, ...
		// The file is not modified.
		// The file is assumed to be in the current directory.
		// The file is assumed to be a text file.

		File file = new File(fileName);
		int fileSize = (int) file.length();
		int splitSize = staticSplitSize * 1024;
		int nbSplits = (int) Math.ceil(fileSize / splitSize);

		BufferedReader br = null;
		BufferedWriter bw = null;

		// Split the file in splitSize KB chunks
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

			String line = null;
			for (int i = 0; i < nbSplits; i++) {
				int byteRead = 0;
				String splitName = splitsSourceDirectory + "/split" + i + ".txt";
				File split = new File(splitName);
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(split), "UTF-8"));
				
				if (line != null) {
					// Write the last read line to the split
					bw.write(line);
					byteRead += line.getBytes().length;
				}
				line = br.readLine();
				while (line != null && byteRead < splitSize) {
					bw.write(line);
					bw.newLine();
					line = br.readLine();
					if (line != null) {
						byteRead += line.getBytes().length;
					}
				}
				bw.close();
				System.out.println("Split " + i + " of " + nbSplits + " done.");
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	

	public static Split makeSplitObject(String fileName, int splitNumber) {
		// Makes a Split object from a file.
		// The file is assumed to be in the splitsSourceDirectory directory.

		File file = new File(splitsSourceDirectory + "/split" + splitNumber + ".txt");
		Split split = new Split();

		try (DataInputStream diStream = new DataInputStream(new FileInputStream(file))) {
			// The "try block" closes the DataInputStream on exit
			// We turn file data to byte data
			long len = (int) file.length();
			byte[] fileBytes = new byte[(int) len];
			int read = 0;
			int numRead = 0;
			while (read < fileBytes.length && (numRead = diStream.read(fileBytes, read, fileBytes.length - read)) >= 0) {
			read = read + numRead;
			}

			// Update the Split object
			split.setFileData(fileBytes);
			split.setFileName("split" + splitNumber);

			try {
				if (addr == null) {
					addr = InetAddress.getLocalHost().getHostAddress();
				}
				split.setSender(addr);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			
			split.setStatus("Success");
			split.setDestinationDirectory(splitsDestinationDirectory);
		} catch (FileNotFoundException e) {
			System.out.println("File not found.");
		} catch (IOException e) {
			System.out.println("Error reading file.");
		}
		return split;
	}

	public static void main(String[] args) {

		machines = machineNames.split("\\s+");

		// Turn array of participating machines into ArrayList
		ArrayList<String> machinesArray = new ArrayList<String>();
		for (String machine: machines) {
			machinesArray.add(machine);
		}
		MachineList machineList = new MachineList(machinesArray);

		// Cleans the splits directory
		cleanDirectory(splitsSourceDirectory);

		// Make splits from the original file
		makeSplitsFromFile(originalFileName);

		// Open a socket connection to all servers
		ArrayList<SocketChannel> sockets = new ArrayList<SocketChannel>();
		for (String machine: machines) {
			try {
				SocketChannel socketOfClient = SocketChannel.open(new InetSocketAddress(machine, port));
				sockets.add(socketOfClient);
				System.out.println("C: Socket created for machine " + machine + " on port " + port);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// Send machine list to all servers
		for (SocketChannel sck : sockets) {
			try { 
				sendObject(sck, machineList);
				System.out.println("C: Machine List sent to " + sck.getRemoteAddress());
			} catch (UnknownHostException e) {
				try {
					System.err.println("C: Don't know about host " + sck.getRemoteAddress());
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			} catch (IOException e) {
				try { 
					System.err.println("C: Couldn't get I/O for the connection to " + sck.getRemoteAddress());
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}

		try {
			Thread.sleep(3000); // This sleep is NECESSARY because we are sending multiple objects
			// Therefore we have to measure time starting from the Reduce operation...
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Send the splits to all servers (no redundancy)
		File dir = new File(splitsSourceDirectory); 
		File[] directoryListing = dir.listFiles();

		int[] n_splits_sent = new int[machines.length];

		for (int i = 0; i < directoryListing.length; i++) {
			try	{
				Split newSplit = makeSplitObject(originalFileName, i);
				int machineToSendSplitToIndex = i % machines.length;
				SocketChannel sck = sockets.get(machineToSendSplitToIndex);
				sendObject(sck, newSplit);
				System.out.println("C: Split " + newSplit.getFileName() + " sent to " + sck.getRemoteAddress());
				n_splits_sent[machineToSendSplitToIndex]++;
				// Thread.sleep(100);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// } catch (InterruptedException e) {
			// 	e.printStackTrace();
			// }
		}

		for (int i = 0; i < machines.length; i++) {
			// Send the number of splits sent to each machine
			sendObject(sockets.get(i), new SplitCount(n_splits_sent[i]));
		}

		for (SocketChannel sck : sockets) { // Close all sockets
			try {
				sck.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return;
		
	// 	// Open server socket on port 12300 to listen to other machines
	// 	// Selector to handle multiple sockets (different ports)
	// 	Selector listenerSelector = null;

	// 	// Selectable channel to bind sockets to
	// 	ServerSocketChannel listenerServerSocket = null;
	// 	try {
	// 		// Open selector and selectable sockets and configure them properly
	// 		listenerSelector = Selector.open();

	// 		// Create a server socket channel and bind it to the port
	// 		listenerServerSocket = ServerSocketChannel.open();
	// 		listenerServerSocket.configureBlocking(false);
	// 		String myName = InetAddress.getLocalHost().getCanonicalHostName();
	// 		InetSocketAddress listenerAddress = new InetSocketAddress(myName, port_words);
	// 		listenerServerSocket.bind(listenerAddress);
	// 		System.out.println("Bound machine " + myName + " on port " + port_words);

	// 		// Register the server socket channel with the selector
	// 		listenerServerSocket.register(listenerSelector, SelectionKey.OP_ACCEPT);
			
	// 	} catch (IOException e) {
	// 		e.printStackTrace();
	// 	}

	// 	// Wait for all servers to finish
	// 	int finishedMachines = 0;
	// 	while (true) {
	// 		try {
	// 			listenerSelector.select(); // Blocks until at least one channel is ready.
	// 			Set<SelectionKey> selectedKeys = listenerSelector.selectedKeys();
	// 			Iterator<SelectionKey> iter = selectedKeys.iterator();
				
	// 			while (iter.hasNext()) {
	// 				// Deal with all connections that are ready at select time.
	// 				SelectionKey key = iter.next();
					
	// 				if (key.isAcceptable()) {
	// 					// Accept a new connection.
	// 					SocketChannel client = listenerServerSocket.accept();
	// 					client.configureBlocking(false);
	// 					client.register(listenerSelector, SelectionKey.OP_READ);
	// 					System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Connection accepted from client: " + client.getRemoteAddress());

	// 					buffers.put(client, ByteBuffer.allocate(CLIENT_BUFFER_SIZE));
						
	// 				} else if (key.isReadable()) {
	// 					// Read object from ByteBuffer
	// 					SocketChannel currentClient = (SocketChannel) key.channel();

	// 					// Read data from the client and put it in the buffer
	// 					// First clear the buffer, then fill it with data.
	// 					ByteBuffer buffer = buffers.get(currentClient);
	// 					buffer.clear();

	// 					// System.out.println("Received buffer" + buffer.toString());
	// 					currentClient.read(buffer); 
	// 					try {
	// 						String wholeString = new String(buffer.array(), 0, buffer.position(), "UTF-8");
	// 						String [] splitString = wholeString.split("\n");

	// 						// Parse string $integer_word$ into integer and word
	// 						for (String s : splitString) {
	// 							if (s.equals("$FINISHED_SENDING_WORDS$")) {
	// 								finishedMachines++;
	// 								if (finishedMachines == machineList.getMachines().size()) {
	// 									// If all machines have finished, we can print the word count and stop the program

	// 									// Sort the words
	// 									List<Map.Entry<String, Integer>> sortedWordCount = new ArrayList<>(words.entrySet());
	// 									Collections.sort(sortedWordCount, new ValueThenKeyComparator<String, Integer>());

	// 									int count = 0;
	// 									for (Map.Entry<String, Integer> entry : sortedWordCount) { // Print the result
	// 										if (count==NUM_WORDS_TO_PRINT) {
	// 											break;
	// 										}
	// 										System.out.println(entry.getKey() + ":" + entry.getValue());
	// 										count++;
	// 									}

	// 									System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + "Done.");
	// 									for (SocketChannel sck : sockets) { // Close all sockets
	// 										try {
	// 											sck.close();
	// 										} catch (IOException e) {
	// 											e.printStackTrace();
	// 										}
	// 									}
	// 									return;
	// 								}
	// 							} else {
	// 								String[] splitWord = s.split("$_");
	// 								String word = splitWord[1];
	// 								int wordCount = Integer.parseInt(splitWord[0]);
	// 								words.put(word, wordCount);
	// 							}
	// 						}

							
	// 					} catch (Exception e) {
	// 						e.printStackTrace();
	// 					}
	// 					}
	// 				}
	// 			} catch (IOException e) {
	// 				e.printStackTrace();
	// 			}
	// 		} 
		
	// 			// Send quit when all splits are sent
	// 			// sendObject(socketOfClient, new Quit(machineList.getMachines().size()));
	// 			// System.out.println("C: Quit sent for machine " + machine);		
			
	}
}
