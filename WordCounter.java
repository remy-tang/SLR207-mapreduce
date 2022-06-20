import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCounter {

	public static void main(String[] args) {
		// Count the number of occurrences of all words in a text file
		HashMap<String, Integer> wordCount = new HashMap<>();

		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		try {
			fis = new FileInputStream("CC-MAIN-20220116093137-20220116123137-00001.warc.wet");
			isr = new InputStreamReader(fis, "UTF-8");
			br = new BufferedReader(isr);

			long startTime = System.currentTimeMillis();
			String nextLine = br.readLine();
			while (nextLine != null) { // Count word occurrences
				String[] words = nextLine.split("[!.:;_,'@?()/ ]"); // Split into word array

//				for (int i=0; i<words.length; i++) { // Convert to lower case
//					words[i] = words[i].toLowerCase();
//				}
				
				for (String word : words) { // Count words in current line
					Integer currCount = wordCount.get(word);
					if (currCount != null) {
						wordCount.put(word, currCount + 1);
					} else {
						wordCount.put(word, 1);
					}
				}
				nextLine = br.readLine(); // Read next line
			}
			
			// Compute elapsed time
			long endTime = System.currentTimeMillis();

			// Sort the words
			List<Map.Entry<String, Integer>> sortedWordCount = new ArrayList<>(wordCount.entrySet());
			Collections.sort(sortedWordCount, new ValueThenKeyComparator<String, Integer>());

			int count = 0;
			for (Map.Entry<String, Integer> entry : sortedWordCount) { // Print the result
				if (count==7) {
					break;
				}
				System.out.println(entry.getKey() + ":" + entry.getValue());
				count++;
			}
			// Print elapsed time
			System.out.println("Elapsed time for counting words: " + (endTime - startTime) + " milliseconds");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {}
	}
}
