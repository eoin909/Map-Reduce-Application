package sampleAssignment;


import java.util.*;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestParallelization{

    public static void main(String[] args) throws IOException {
        //Array of filenames to be tested
        try{
            int threadPoolSize = Integer.parseInt(args[0]);
            mapReduce(threadPoolSize);
        } catch (ArrayIndexOutOfBoundsException e){
            System.err.println("ERROR: No thread pool size argument provided");
            e.printStackTrace();
        }
    }

    private static void mapReduce(int threadPoolSize) throws IOException {
        long beginTime = System.nanoTime();
        String[] files = {"chapter_sherlock.txt", "Sherlock_Holmes.txt", "The_Bhagavad_Gita.txt"};

        Map<String, String> input = readFileContent(files);

        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<>();

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
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            final GroupCallback<Map<String, List<String>>> groupCallback = new GroupCallback<Map<String, List<String>>>() {

                @Override
                public synchronized void groupDone(String k, List<String> v) {
                    groupedItems.put(k, v);
                }
            };


            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while (mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);

                threadPool.execute(() -> {
                    group(word, file, list, groupCallback);
                });

            }

            threadPool.shutdown();
            while (!threadPool.isTerminated()) ;

            threadPool = Executors.newFixedThreadPool(threadPoolSize);

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                threadPool.execute(() -> reduce(word, list, reduceCallback));
            }

            threadPool.shutdown();
            // Add timeont of 1 minute to thread pool
            try{
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (!threadPool.isTerminated()) {}
            long finishTime = System.nanoTime();
            long timePeriod = (finishTime - beginTime) / 1000;
            System.out.println(output);
            System.out.printf("Theard pool Size:\t%d\t Total time taken: %d μs %n", threadPoolSize, timePeriod);
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

    private static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }

        callback.reduceDone(word, reducedList);
    }
    public static void group(String word, String file,  List<String> list, GroupCallback<Map<String, List<String>>> groupCallback) {

        if (list == null) {
            list = new LinkedList<String>();
        }
        list.add(file);
        groupCallback.groupDone(word, list);
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