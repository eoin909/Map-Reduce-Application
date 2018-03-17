
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class part1 {

	public static void main(String[] args) throws IOException {

		part1 mapReduce = new part1();
		// initialize variables
		int threadCount = 0;
		String string1 = null;
		String string2 = null;
		String string3 = null;

		try {
			threadCount = Integer.parseInt(args[0]);

		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("ERROR: No thread pool size argument provided");
			e.printStackTrace();
		}

		try {
			string1 = args[1];
			string2 = args[2];
			string3 = args[3];
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("ERROR: No File Location argument provided");
			e.printStackTrace();
		}

		File fileLocation1 = new File(string1);
		File fileLocation2 = new File(string2);
		File fileLocation3 = new File(string3);

		String file1Contents = mapReduce.readFileContent(fileLocation1);
		String file2Contents = mapReduce.readFileContent(fileLocation2);
		String file3Contents = mapReduce.readFileContent(fileLocation3);

		// Set number of threads to be executed to imported value.
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);

		long start = System.nanoTime();

		Map<String, String> content = new HashMap<String, String>();

		content.put(fileLocation1.getName(), file1Contents);
		content.put(fileLocation2.getName(), file2Contents);
		content.put(fileLocation3.getName(), file3Contents);

		// APPROACH #3: Distributed MapReduce
		final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

		final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
			@Override
			public synchronized void mapDone(String file, List<MappedItem> results) {
				mappedItems.addAll(results);
			}
		};

		List<Thread> mapCluster = new ArrayList<Thread>(content.size());

		Iterator<Map.Entry<String, String>> inputIter = content.entrySet().iterator();
		while (inputIter.hasNext()) {
			Map.Entry<String, String> entry = inputIter.next();
			final String file = entry.getKey();
			final String contents = entry.getValue();

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					map(file, contents, mapCallback);
				}
			});
			// Add Thread t to List of threads.
			mapCluster.add(t);
		}

		// Iterate through the list and execute each thread.
		for (Thread t : mapCluster) {
			executor.execute(t);
		}

		// When finished shut down all the threads.
		executor.shutdown();
		// Wait until executor is finished and shutdown.
		while (!executor.isTerminated())
			;

		// GROUP:
		Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

		Iterator<MappedItem> mappedIter = mappedItems.iterator();
		while (mappedIter.hasNext()) {
			MappedItem item = mappedIter.next();
			String word = item.getWord();
			String file = item.getFile();
			List<String> list = groupedItems.get(word);
			if (list == null) {
				list = new LinkedList<String>();
				groupedItems.put(word, list);
			}
			list.add(file);
		}

		/*******************************************************************************************
		 * REDUCE:
		 */
		final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
			@Override
			public synchronized void reduceDone(String k, Map<String, Integer> v) {
				output.put(k, v);
			}
		};

		// Re-initialise executor object.
		executor = Executors.newFixedThreadPool(5);

		List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

		Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
		while (groupedIter.hasNext()) {
			Map.Entry<String, List<String>> entry = groupedIter.next();
			final String word = entry.getKey();
			final List<String> list = entry.getValue();

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					reduce(word, list, reduceCallback);
				}
			});
			// Add Thread t to List of threads.
			reduceCluster.add(t);
		}

		// Iterate through the list and execute each thread.
		for (Thread t : reduceCluster) {
			executor.execute(t);
		}

		// When finished shut down all the threads.
		executor.shutdown();
		// Wait until executor is finished and shutdown.
		while (!executor.isTerminated())
			;

		System.out.println(output);
		System.out.println("Size of threadpool \n-> " + threadCount);
		long finish = System.nanoTime();
		double seconds = ((double) (finish - start) / 1000000000);

		System.out.println("Time taken to complete task \n-> " + seconds + " SECONDS");
		System.out.println("Part 2 - Uses callbacks");
	}

	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		String[] words = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(words.length);
		for (String word : words) {
			results.add(new MappedItem(word, file));
		}
		callback.mapDone(file, results);
	}

	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(word, reducedList);
	}

	// Reads content of file
	public String readFileContent(File file) throws IOException {

		Scanner fileScanner = null;
		StringBuilder stringBuilder = new StringBuilder();

		try {
			fileScanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (fileScanner.hasNextLine()) {
			Scanner fileScanner2 = new Scanner(fileScanner.nextLine());
			while (fileScanner2.hasNext()) {
				String string = fileScanner2.next();
				stringBuilder.append(string + " ");
			}
		}
		return stringBuilder.toString();
	}
}