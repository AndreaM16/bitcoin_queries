package bitcoin.queries;

import org.apache.log4j.BasicConfigurator;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MainQuery_2 {

    public static void main(String[] args) throws IOException, InterruptedException {

        BasicConfigurator.configure();
        // Initalize bitcoinJ
        BriefLogFormatter.init();
        NetworkParameters networkParameters = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());
        long startTime = System.currentTimeMillis();
        ArrayList<Transaction> t_list= new ArrayList<>();
        HashMap<String, ArrayList<Transaction>> identifier_transactions = new HashMap<>();
        HashMap<String, ArrayList<Transaction>> map = new HashMap<>();

        // Read the blockchain files from the disk
        List<File> blockChainFiles = new LinkedList<File>();
        for (int i = 0; true; i++) {
            File file = new File(Settings.BLOCKCHAIN_PATH + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            blockChainFiles.add(file);
        }
        BlockFileLoader bfl = new BlockFileLoader(networkParameters, blockChainFiles);

        String hash_last_block = "0000000000000000228ffe0a981f4e33225eec2d303a8794585bde3d5a6805b0";
        Pattern p = Pattern.compile("\\[(.*?)\\]");

        for (Block block : bfl) {

            if (block.getHashAsString().equals(hash_last_block)) {
                break;
            }

            for (Transaction t : block.getTransactions()) {

                try {
                    List<TransactionOutput> s = t.getOutputs();

                    for (TransactionOutput l : s) {
                        if (l.getScriptPubKey().isOpReturn()) {


                            t_list.add(t);

                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }


        }

        for (Transaction t: t_list){
            for(TransactionOutput out: t.getOutputs()){

                Matcher matcher=p.matcher(out.getScriptPubKey().toString());

                if(matcher.find()) {

                    String identifier = null;
                    try {

                        identifier = matcher.group(1).substring(0, 4);

                    } catch (Exception e) {
                    }
                    if (identifier != null) {

                        if (identifier_transactions.containsKey(identifier)) {

                            if (!(identifier_transactions.get(identifier).indexOf(t) > -1) ){

                                identifier_transactions.get(identifier).add(t);
                            }

                        } else {

                            ArrayList<Transaction> t_listTemp = new ArrayList<>();
                            t_listTemp.add(t);
                            identifier_transactions.put(identifier, t_listTemp);
                        }
                    }
                }
            }

        }

        map = getMapSortedByListSize(identifier_transactions);

        save_indentifierToFile(map);

        System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) / 1000);

    }


    private static HashMap<String, ArrayList<Transaction>> getMapSortedByListSize(HashMap<String, ArrayList<Transaction>> map) {
        return map.entrySet().stream()
                .sorted(Collections.reverseOrder(Comparator.comparingInt(e -> e.getValue().size())))
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

    private static void save_indentifierToFile(HashMap<String, ArrayList<Transaction>> map) throws IOException {
        File f = new File(Settings.QUERY2_PATH + "result.txt");
        FileWriter w = new FileWriter(f);

        for (HashMap.Entry<String, ArrayList<Transaction>> entry : map.entrySet())
        {
            w.append("identifier: "+entry.getKey() + "  number of transactions: "+ entry.getValue().size() +"\n");
        }
        w.flush();
        w.close();
    }
}
