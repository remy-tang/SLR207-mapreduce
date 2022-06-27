import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.net.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class SimpleClient {

	private static String addr = "137.194.252.46";	// Client machine address.
	private static int port = 12302;

	// List of all machines participating
	// String machineNames = "tp-1a226-12.enst.fr tp-1a226-14.enst.fr tp-1a226-16.enst.fr";
	private static String machineNames = "tp-1a226-12.enst.fr tp-1a226-14.enst.fr tp-1a226-16.enst.fr";

	private static String[] machines;

	private static String splitsSourceDirectory = "./little_splits";

	private static String splitsDestinationDirectory = "/tmp/retang/splits/";

	private static String originalFileName = "./split0.txt";

	private static int staticSplitSize = 32; // splitSize in KB

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

			// Write the ByteArrayOutputStream to the SocketChannel
            buffer = ByteBuffer.wrap(baos.toByteArray());
            client.write(buffer);

			buffer.clear();

            // Close the streams
            oosSendObject.close();
            baos.close();

			try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


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
		// Makes splits of 64KB from a file.
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
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

			String line = null;
			for (int i = 0; i < nbSplits; i++) {
				String splitName = splitsSourceDirectory + "/split" + i + ".txt";
				File split = new File(splitName);
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(split)));
				
				if (line != null) {
					// Write the last line to the split
					bw.write(line);
				}
				line = br.readLine();
				int byteRead = line.getBytes().length;
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
			split.setSender(addr);
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

		// // Cleans the splits directory
		// cleanDirectory(splitsSourceDirectory);

		// // Make splits of 32KB from the original file
		// makeSplitsFromFile(originalFileName);

		ArrayList<SocketChannel> sockets = new ArrayList<SocketChannel>();
		for (String machine: machines) { // Open a socket connection to all servers
			try {
				SocketChannel socketOfClient = SocketChannel.open(new InetSocketAddress(machine, port));
				sockets.add(socketOfClient);
				System.out.println("C: Socket created for machine " + machine + " on port " + port);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		for (SocketChannel sck : sockets) { // Send machine list to all servers
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


		// // Open server socket on port 12300 to listen to other machines
		// // Selector to handle multiple sockets (different ports)
		// Selector listenerSelector = null;

		// // To received objects from the input stream.
		// Object receivedObject = null;

		// // Selectable channel to bind sockets to
		// ServerSocketChannel listenerServerSocket = null;
		// try {
		// 	// Open selector and selectable sockets and configure them properly
		// 	listenerSelector = Selector.open();

		// 	// Create a server socket channel and bind it to the port
		// 	listenerServerSocket = ServerSocketChannel.open();
		// 	listenerServerSocket.configureBlocking(false);
		// 	String myName = InetAddress.getLocalHost().getCanonicalHostName();
		// 	InetSocketAddress listenerAddress = new InetSocketAddress(myName, port);
		// 	listenerServerSocket.bind(listenerAddress);
		// 	System.out.println("Bound machine " + myName + " on port " + port);

		// 	// Register the server socket channel with the selector
		// 	listenerServerSocket.register(listenerSelector, SelectionKey.OP_ACCEPT);
			
		// } catch (IOException e) {
		// 	e.printStackTrace();
		// }

		// // Wait for all servers to finish

		// int finishedMachines = 0;
		// while (true) {
		// 	try {
		// 		listenerSelector.select(); // Blocks until at least one channel is ready.
		// 		Set<SelectionKey> selectedKeys = listenerSelector.selectedKeys();
		// 		Iterator<SelectionKey> iter = selectedKeys.iterator();
		// 		while (iter.hasNext()) {
		// 			// Deal with all connections that are ready at select time.
		// 			SelectionKey key = iter.next();
					
		// 			if (key.isAcceptable()) {
		// 				// Accept a new connection.
		// 				SocketChannel client = listenerServerSocket.accept();
		// 				client.configureBlocking(false);
		// 				client.register(listenerSelector, SelectionKey.OP_READ);
		// 				System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Connection accepted from client: " + client.getRemoteAddress());
		// 			} else if (key.isReadable()) {
		// 				// Read object from ByteBuffer
		// 				SocketChannel currentClient = (SocketChannel) key.channel();

		// 				// Buffer to read data (objects)
		// 				ByteBuffer buffer = ByteBuffer.allocate(2048);
		// 				currentClient.read(buffer); // Read data from the client and put it in the buffer.
						
		// 				if (buffer.position() == 0) {
		// 					// If the buffer is empty, we skip this iteration.
		// 					iter.remove();
		// 					continue;
		// 				}

		// 				// Open object stream to get objects back
		// 				ObjectInputStream byteOos = null;
		// 				try {
		// 					byteOos = new ObjectInputStream(new ByteArrayInputStream(buffer.array()));
		// 					receivedObject = byteOos.readObject();
		// 				} catch (ClassNotFoundException e) {
		// 					e.printStackTrace();
		// 				} catch (Exception e) {
		// 					System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Error when creating ObjectInputStream");
		// 					e.printStackTrace();
		// 				} finally {
		// 					byteOos.close();
		// 				}
						
		// 				// System.out.println("receivedObject: " + receivedObject.toString());
		// 				// // Deal with received object
		// 				// if (receivedObject instanceof HashMap) {
		// 				// 	finishedMachines++;
		// 				// 	try { 
		// 				// 		HashMap<String, Integer> words = (HashMap<String, Integer>)receivedObject;
		// 				// 		for (String word : words.keySet()) {
		// 				// 			int num = words.get(word);
		// 				// 			System.out.println("Word " + word + " found " + num + " times");
		// 				// 		}
		// 				// 	} catch (ClassCastException e) {
		// 				// 		System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " LR: Error when casting object to HashMap");
		// 				// 		e.printStackTrace();
		// 				// 	}

		// 				// 	if (finishedMachines == machineList.getMachines().size()) {
		// 				// 		// If all machines have finished, we can stop the program
		// 				// 		System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + "All machines have finished");
		// 				// 		return;
		// 				// 	}
		// 				// }
		// 			}
		// 		}
		// 	} catch (IOException e) {
		// 		e.printStackTrace();
		// 	}
		// }
		
				// Send quit when all splits are sent
				// sendObject(socketOfClient, new Quit(machineList.getMachines().size()));
				// System.out.println("C: Quit sent for machine " + machine);			
	}
}
