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

        File merge_file_path = new File(Settings.QUERY1_PATH + "/merge_query_1.txt");

        if (merge_file_path.length() == 0) {
            // Read the blockchain files from the disk
            List<File> blockChainFiles = new LinkedList<File>();
            for (int i = 0; true; i++) {
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
                res.add(service.submit(new Query_1(blockChainFiles, networkParameters, i, Context.get(), min_a[i-1], max_a[i-1])));
            }
            service.shutdown();


            for ( Future<HashMap<String,ArrayList<Transaction>>> fut : res ){
                identifier_transactions.putAll(fut.get());
            }



            //List of all files in Q1 path
            File folder = new File(Settings.QUERY1_PATH);
            File[] file_list = folder.listFiles();

            FileWriter fr = new FileWriter(merge_file_path, true);
            BufferedWriter br = new BufferedWriter(fr);

            //For each file
            for (File file : file_list) {
                FileInputStream fis = new FileInputStream(file);
                BufferedReader buf_reader = new BufferedReader(new InputStreamReader(fis));
                String line;
                while ((line = buf_reader.readLine()) != null) {
                    br.write(line);
                    br.newLine();
                }

                buf_reader.close();
            }

            System.out.println("Merge Done!");


            br.close();
        }

        System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) / 1000);
    }


}
