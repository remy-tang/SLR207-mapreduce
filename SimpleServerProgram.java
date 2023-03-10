import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.channels.SocketChannel;

/*	Opens socket connection to the client.
	Opens input an output streams.
	Deploys listener and worker threads to listen to incoming data,
	perform necessary computations, and send data back to client.
*/
public class SimpleServerProgram {

	// Variables that are initialized by ListenerReducer upon receiving the corresponding data.
	// They are then accessed by WorkerSender, hence the volatile keyword.

	// List of all the machines.
	private static MachineList machines = new MachineList();

	// Words that belong to this machine (shuffle step) and their count (reduce step).
	private static volatile HashMap<String, Integer> myWords = new HashMap<String, Integer>();

	// Save the client address to send back the results.
	private static volatile SocketChannel clientSocket;

	public static void main(String args[]) {

		// Queue to send strings from ListenerReducer to WorkerSender threads.
		ConcurrentLinkedQueue<String> splitQueue = new ConcurrentLinkedQueue<String>();

		// Pipes to send information from ListenerReducer to WorkerSender threads.
		PipedOutputStream pos = new PipedOutputStream();
		PipedInputStream pis = new PipedInputStream();

		try {
			// Connect reader and writer pipes
			pos.connect(pis);
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}

		// Dispatch ListenerReducer and WorkerSender threads.
		ListenerReducer listenerReducer = new ListenerReducer(machines,
																myWords, 
																pos, 
																clientSocket,
																splitQueue);
	
		WorkerSender workerSender = new WorkerSender(machines,
														myWords,
														pis, 
														clientSocket,
														splitQueue);

		listenerReducer.start();
		workerSender.start();

		try {
			// Wait for both threads to finish
			listenerReducer.join();
			workerSender.join();

			// Close the pipes
			pos.close();
			pis.close();

			// Exit once finished
			return;
		} catch (InterruptedException ie) {
			System.out.println(ie);
			ie.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
    }
}