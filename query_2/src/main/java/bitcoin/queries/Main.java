package bitcoin.queries;

import org.apache.log4j.BasicConfigurator;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        BasicConfigurator.configure();
        // Initalize bitcoinJ
        BriefLogFormatter.init();
        NetworkParameters networkParameters = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());
        long startTime = System.currentTimeMillis();
        HashMap<String, ArrayList<Transaction>> identifier_transactions = new HashMap<>();
        HashMap<String, ArrayList<Transaction>> map_result = new HashMap<>();

            // Read the blockchain files from the disk
            List<File> blockChainFiles = new LinkedList<File>();
            for (int i =0; true; i++) {
                File file = new File(Settings.BLOCKCHAIN_PATH + String.format(Locale.US, "blk%05d.dat", i));
                if (!file.exists())
                    break;
                blockChainFiles.add(file);
            }

            int[] min_a = { 0, 77251, 154501, 231751 };
            int[] max_a = { 77250, 154500, 231750, 309000 };

            ExecutorService service = Executors.newFixedThreadPool(4);
            ArrayList<Future<HashMap<String,ArrayList<Transaction>>>> res = new ArrayList<>();
            for (int i=1; i<5; i++){
                res.add(service.submit(new Query_2(blockChainFiles, networkParameters, i, Context.get(), min_a[i-1], max_a[i-1])));
            }
            service.shutdown();


            for ( Future<HashMap<String,ArrayList<Transaction>>> fut : res ){
                identifier_transactions.putAll(fut.get());
            }

        map_result = getMapSortedByListSize(identifier_transactions);
        save_indentifierToFile(map_result);

        System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) / 1000);

    }


    private static HashMap<String, ArrayList<Transaction>> getMapSortedByListSize(HashMap<String, ArrayList<Transaction>> map) {
        return map.entrySet().stream()
                .sorted(Collections.reverseOrder((e1, e2) -> e1.getValue().size() - e2.getValue().size()))
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

   private static void save_indentifierToFile(HashMap<String, ArrayList<Transaction>> map) throws IOException {
        File f = new File(Settings.QUERY2_PATH + "query_2.txt");
        FileWriter w = new FileWriter(f);
        for (HashMap.Entry<String, ArrayList<Transaction>> entry : map.entrySet())
        {
            w.append("identifier: "+entry.getKey() + "  number of transactions: "+ entry.getValue().size() +"\n");
        }
        w.flush();
        w.close();
    }
}
