import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.ObjectOutputStream;
import java.io.FileReader;
import java.net.*;

/*  Performs the Map step of the MapReduce algorithm.
 
*/
public class WorkerSender extends Thread {

    private static int port = 12302;

    // Save all the machines
    private final MachineList machines;

    // Words that belong to this machine (shuffle step) and their count (reduce step).
	private final HashMap<String, Integer> myWords;

    // Pipe to receive data from ListenerReducer
    private final PipedInputStream pis;

    // Address of the client.
	private final String clientAddr; 

    // Queue to send strings from ListenerReducer to WorkerSender threads.
	private final ConcurrentLinkedQueue<String> splitQueue;

    private static SocketChannel[] clients;

    WorkerSender(MachineList machines, 
                 HashMap<String, Integer> myWords,
                 PipedInputStream pis,
                 String clientAddr,
                 ConcurrentLinkedQueue<String> splitQueue) {
        this.machines = machines;
        this.myWords = myWords;
        this.pis = pis;
        this.clientAddr = clientAddr;
        this.splitQueue = splitQueue;
    }

    public void sendObject(String hostname, Object object) {
        /** Sends an object to a machine via its hostname.
            Called by WorkerSender.java.
            The object is sent as a byte array.
        */

        ByteArrayOutputStream baos = null;
        ObjectOutputStream oosSendObject = null;

        try {
            int index = machines.getMachines().indexOf(hostname);

            // Send word to the correct machine from hostname
            baos = new ByteArrayOutputStream();
            oosSendObject = new ObjectOutputStream(baos);
            oosSendObject.writeObject(object);
            oosSendObject.flush();

            ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
            clients[index].write(buffer);
            buffer.clear();
            
            // Close the streams
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

    public static void sendObject(SocketChannel client, Object object) {
        /** Sends an object to a machine via its SocketChannel.
            Called by WorkerSender.java.
            The object is sent as a byte array.
        */

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

    public String encode(String word) {
        /** Encodes a word as an integer left-padded with zeros.
            Called by WorkerSender.java.
        */

        // "Hashing" function
        word = word.toLowerCase();
        StringBuilder encodedSplitWord = new StringBuilder();

        for (int i=0; i<word.length(); i++) {
            char letter = word.charAt(i);
            int encodedLetter = (int)letter + 1 - (int)'a';
            if (encodedLetter >= 0 && encodedLetter <= 26) { 
                String encoding = String.format("%02d", encodedLetter);
                encodedSplitWord.append(encoding);
            }
        }

        return encodedSplitWord.toString();
    }

    public void run() {
        System.out.println("WR: WorkerSender started, waiting to receive data from LR...");

        // Communicated event from ListenerReducer
        // 1 to create socket connections to all machines
        // 2 to read split and send words
        // 3 
        int listenerEvent = -1;
               
        while(true) {

            try {
                // Read split file information from pipe.
                // The read operation is blocking so polling is avoided.
                listenerEvent = pis.read();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            if (listenerEvent == 1) {
                try {
                    System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: List of machines acknowledged");
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                // Open socket connection to all machines.
                clients = new SocketChannel[machines.getMachines().size()];
                for (int i=0; i<machines.getMachines().size(); i++) {
                    String machineName = machines.getMachine(i);
                    try {
                        System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: Trying to connect to " + machineName);
                        clients[i] = SocketChannel.open(new InetSocketAddress(machineName, port));
                        System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: Connected to " + machineName);
                    } catch (IOException e) {
                        // Retry connection to the machine.
                        try {
                            System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: Failed to connect to " + machineName);
                        } catch (UnknownHostException e1) {
                            e1.printStackTrace();
                        }
                        i--;
                        break;
                    } 
                }

            } else if (listenerEvent == 2) {
                // Read split and words to correct machine.
                // Retrieve the head of the queue and remove it from the queue.
                String outputFile = splitQueue.poll();

                // To read the file.
                FileReader fr = null;
                BufferedReader br = null;

                try {
                    fr = new FileReader(outputFile);
                    br = new BufferedReader(fr);
                    String currentLine = br.readLine();
                    while (currentLine != null) {
                        // Hash each word
                        String[] currentWords = currentLine.split("[!.:;_,'@?()/° ]");
                        // String[] currentWords = currentLine.split(" ?(?<!\\G)((?<=[^\\p{Punct}])(?=\\p{Punct})|\\b) ?");
                        for (String wordInit : currentWords) {
                            String wordAsInt = encode(wordInit);

                            if (wordAsInt.length() > 0) { // If the word is not empty.
                                // Send result to correct machine
                                BigInteger encodedWord = new BigInteger(wordAsInt);
                                String wordMachine = machines.getMachines().get(encodedWord.mod(BigInteger.valueOf(machines.getMachines().size())).intValue());
                                Word currentEncodedWord = new Word(wordInit);
                                try {
                                    System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS : sending word '" + currentEncodedWord.getWord() + "' to " + wordMachine);
                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                }
                                this.sendObject(wordMachine, currentEncodedWord);
                            }

                        }
                        currentLine = br.readLine();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        fr.close();
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else if (listenerEvent == 3) {
                // Broadcast to all that you have finished your splits
                try {
                    System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: Sending broadcast to all machines");
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                for (String machine : machines.getMachines()) {
                    FinishedMachine finishedSplit = new FinishedMachine();
                    this.sendObject(machine, finishedSplit);
                }
            
            } else if (listenerEvent == 4) {
                // If all servers have finished their split including this one.
                // Send the words with their count to the client.
                try {
                    for (String word : myWords.keySet()) {
                        int num = myWords.get(word);
                        System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " Word " + word + " found " + num + " times");
                        
                    }
                    return;
                    // System.out.println("WS: All servers finished, sending words to client");
                    // InetAddress host = InetAddress.getByName("137.194.252.46");
                    // System.out.println(host.getHostName());
                    // SocketChannel originalClient = SocketChannel.open(new InetSocketAddress(host.getHostName(), port));
                    // sendObject(originalClient, myWords);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // Exit
                return;
            
            }
        }
    }
}
