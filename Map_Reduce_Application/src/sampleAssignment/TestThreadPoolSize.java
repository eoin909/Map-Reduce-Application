package sampleAssignment;

import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

public class TestThreadPoolSize {
	
    public static void main (String[] args) throws IOException {
    	
    	// initialize variables
        int loopCount = 0;
		int maxThreadCount = 0;
		
		try {
			maxThreadCount = Integer.parseInt(args[0]);
			loopCount = Integer.parseInt(args[1]);

		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("ERROR: No thread pool size argument provided");
			e.printStackTrace();
		}

        long beginTime;
        long finishTime;
        long total = 0;
        long meanTime;
        for (int threadPoolSize = 1; threadPoolSize <= maxThreadCount; threadPoolSize++) {

            for (int i = 0; i < loopCount; i++) {
            	beginTime = System.nanoTime();
                mapReduce(threadPoolSize);
                finishTime = System.nanoTime();
                total += (finishTime - beginTime);
            }
            
            meanTime = (total / loopCount);
            System.out.printf("No. Threads:\t%d\t Mean Time: %d μs %n", threadPoolSize, meanTime);
            total = 0;
        }
        System.out.printf("max Thread Count:\t%d\t Loop Counter: %d%n", (maxThreadCount), loopCount);

    }

    private static void mapReduce(int threadPoolSize) throws IOException {
        String[] files = {"chapter_sherlock.txt", "Sherlock_Holmes.txt", "The_Bhagavad_Gita.txt"};

        Map<String, String> input = readFileContent(files);

        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

            for (Map.Entry<String, String> entry : input.entrySet()) {
                final String file = entry.getKey();
                final String contents = entry.getValue();

                threadPool.execute(() -> map(file, contents, mapCallback));
            }

            threadPool.shutdown();
            try{
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!threadPool.isTerminated()) {}

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<>();

            for (MappedItem item : mappedItems) {
                String entry = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(entry);
                if (list == null) {
                    list = new LinkedList<>();
                    groupedItems.put(entry, list);
                }
                list.add(file);
            }

            // REDUCE:
        
	            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
	            while(groupedIter.hasNext()) {
	                    Map.Entry<String, List<String>> entry = groupedIter.next();
	                    String word = entry.getKey();
	                    List<String> list = entry.getValue();
	                    
	                    reduce(word, list, output);
	            }
            
            
        }
    }
    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
                results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }
    
    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
                Integer occurrences = reducedList.get(file);
                if (occurrences == null) {
                        reducedList.put(file, 1);
                } else {
                        reducedList.put(file, occurrences.intValue() + 1);
                }
        }
        output.put(word, reducedList);
    }
	
    // Reads content of file 
    // returns contents in a hash map
	public static Map<String, String>  readFileContent(String[] files) throws IOException {
        Map<String, String> inputMap = new HashMap<>();
        for (String file : files) {
        	BufferedReader buff = new BufferedReader(new FileReader(file));
            String string;
            String content = "";
            String[] stringSplit;

            while ((string = buff.readLine()) != null) {
            	stringSplit= string.split("\\s+");

                for (int j = 0; j < stringSplit.length; j++) {
                	stringSplit[j] = stringSplit[j].toUpperCase().replaceAll("[^a-zA-Z]", "");
                    content += stringSplit[j] + " ";
                }
            }
            inputMap.put(file, content);
        }
       return inputMap;
	}	
}
