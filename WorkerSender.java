import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.ObjectOutputStream;
import java.io.FileReader;
import java.net.*;
import java.time.Duration;
import java.time.Instant;

/*  Performs the Map step of the MapReduce algorithm.
 
*/
public class WorkerSender extends Thread {

    private static int NUM_WORDS_TO_PRINT = 20;

    private static int port = 12302;

    // private static int port_word = 12321;
    // private static String clientHostname = "SweetMango";
    // Address of the client.
	private final SocketChannel clientSocket; 

    // Save all the machines
    private final MachineList machines;

    // Words that belong to this machine (shuffle step) and their count (reduce step).
	private final HashMap<String, Integer> myWords;

    // Pipe to receive data from ListenerReducer
    private final PipedInputStream pis;

    // Queue to send strings from ListenerReducer to WorkerSender threads.
	private final ConcurrentLinkedQueue<String> splitQueue;

    private static SocketChannel[] clients;

    WorkerSender(MachineList machines, 
                 HashMap<String, Integer> myWords,
                 PipedInputStream pis,
                 SocketChannel clientSocket,
                 ConcurrentLinkedQueue<String> splitQueue) {
        this.machines = machines;
        this.myWords = myWords;
        this.pis = pis;
        this.clientSocket = clientSocket;
        this.splitQueue = splitQueue;
    }

    public void sendString(String hostname, String word) {
        /** Sends a string to a client.
            Called by WorkerSender.java.
        */
        
        try {
            word = word + "\n"; // Add newline to end of word to separate words
            ByteBuffer buffer = ByteBuffer.wrap(word.getBytes());

            int index = machines.getMachines().indexOf(hostname);
            clients[index].write(buffer);

            // System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + "Sending " + buffer.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // try {
        //     Thread.sleep(100);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }SS
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

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // buffer.clear();

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
        Instant start = null;

        System.out.println("WR: WorkerSender started, waiting to receive data from LR...");

        // Communicated event from ListenerReducer
        // 1 to create socket connections to all machines
        // 2 to read split and send words
        // 3 to broadcast that all splits have been computed
        // 4 to print the final word count
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
                start = Instant.now();

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
                        String[] currentWords = currentLine.split("[!.:;_,'@?()/° \n\t]+");
                        // String[] currentWords = currentLine.split(" ?(?<!\\G)((?<=[^\\p{Punct}])(?=\\p{Punct})|\\b) ?");
                        for (String wordInit : currentWords) {
                            String wordAsInt = encode(wordInit);

                            if (wordAsInt.length() > 0) { // If the word is not empty.
                                // Send result to correct machine
                                BigInteger encodedWord = new BigInteger(wordAsInt);
                                String wordMachine = machines.getMachines().get(encodedWord.mod(BigInteger.valueOf(machines.getMachines().size())).intValue());
                                // try {
                                //     System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS : sending word '" + wordInit + "' to " + wordMachine);
                                // } catch (UnknownHostException e) {
                                //     e.printStackTrace();
                                // }
                                this.sendString(wordMachine, wordInit);
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
                    System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: Broadcast to all machines that all splits have been computed");
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                for (String machine : machines.getMachines()) {
                    this.sendString(machine, "$FINISHED_SPLITS$\n");
                }
            
            } else if (listenerEvent == 4) {

                // This part does not work, instead we print to the console.

                // If all servers have finished their split including this one.
                // // Send the words with their count to the client.
                // InetSocketAddress cliAddress = new InetSocketAddress(clientHostname, port_word);
                
                // try {
                //     SocketChannel clientf = SocketChannel.open(cliAddress);
                //     for (String word : myWords.keySet()) {
                //         int num = myWords.get(word);
                //         // System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " : Counted " + num + " " + word);
                //         String clientWord = "$" + num + "_" + word + "$\n"; // Add newline to end of word to separate words
                //         ByteBuffer clientBuffer = ByteBuffer.wrap(clientWord.getBytes());
                //         clientf.write(clientBuffer);
                //     }
                //     String finishedSendingWords = "$FINISHED_SENDING_WORDS$\n";
                //     ByteBuffer finalClientBuffer = ByteBuffer.wrap(finishedSendingWords.getBytes());
                //     clientf.write(finalClientBuffer);
                // } catch (Exception e) {
                //     e.printStackTrace();
                // }

                // Print the words with their count to the console.

                // Sort the words
                List<Map.Entry<String, Integer>> sortedWordCount = new ArrayList<>(myWords.entrySet());
                Collections.sort(sortedWordCount, new ValueThenKeyComparator<String, Integer>());

                int count = 0;
                for (Map.Entry<String, Integer> entry : sortedWordCount) { // Print the words with their count.
                    if (count==NUM_WORDS_TO_PRINT) {
                        break;
                    }
                    System.out.println("Counted : " + entry.getKey() + ":" + entry.getValue());
                    count++;
                }

                // Exit
                try {
                    Instant end = Instant.now();
                    System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " WS: Finished in " + Duration.between(start, end).toMillis() + "ms");
                    System.out.println(InetAddress.getLocalHost().getCanonicalHostName() + " Task complete, ending thread");
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                return;
            
            }
        }
    }
}
