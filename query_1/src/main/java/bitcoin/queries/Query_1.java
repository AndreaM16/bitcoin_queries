package bitcoin.queries;

import org.bitcoinj.core.*;
import org.bitcoinj.utils.BlockFileLoader;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Query_1 implements Callable<HashMap<String,ArrayList<Transaction>>> {

    private List<File> blockChainFiles;
    private NetworkParameters networkParameters;
    private int index = 0;
    private int min = 0;
    private int max = 0;
    private Context context;

    public Query_1(List<File> blockChainFiles, NetworkParameters networkParameters, int index, Context context, int min, int max) {

        this.blockChainFiles = blockChainFiles;
        this.networkParameters = networkParameters;
        this.index = index;
        this.context = context;
        this.min = min;
        this.max = max;

    }

    public HashMap<String, ArrayList<Transaction>> call() throws IOException {

        Context.propagate(context);
        System.out.println("Thread " + index + " started!");

        String file_path = Settings.QUERY1_PATH + "/" + index + ".txt";

        BlockFileLoader bfl = new BlockFileLoader(networkParameters, blockChainFiles);

        File f = new File(file_path);
        FileWriter w = new FileWriter(f);
        int height = 1;

        Pattern p = Pattern.compile("\\[(.*?)\\]");

        HashMap<String, ArrayList<Transaction>> identifier_transactions = new HashMap<>();
        String script_string;

        for (Block block : bfl) {
            if (height >= min && height <= max) {
                if (height == max + 1) break;
                for (Transaction t : block.getTransactions()) {
                    try {
                        List<TransactionOutput> s = t.getOutputs();
                        for (TransactionOutput l : s) {
                            script_string = l.toString();
                            if (script_string.contains("RETURN")) {
                                writeToFile(w, t);
                                Matcher matcher = p.matcher(l.getScriptPubKey().toString());
                                if (matcher.find()) {
                                    String identifier = null;
                                    try {
                                        identifier = matcher.group(1).substring(0, 4);
                                    } catch (Exception e) {
                                        //e.printStackTrace();
                                    }
                                    if (identifier_transactions.containsKey(identifier) && identifier != null) {
                                        ArrayList<Transaction> t_list = identifier_transactions.get(identifier);
                                        t_list.add(t);
                                    } else if (identifier != null) {
                                        ArrayList<Transaction> t_list = new ArrayList<>();
                                        t_list.add(t);
                                        identifier_transactions.put(identifier, t_list);
                                    }
                                }

                            }
                        }
                    } catch (Exception e) {
                        //e.printStackTrace();
                    }
                }
            }

            height++;

        }

        w.flush();
        w.close();

        System.out.println("Thread " + index + " finished!");
        return identifier_transactions;

    }

    private void writeToFile(FileWriter file, Transaction t) throws IOException {
        file.append(t.getHashAsString() + "\n");
    }
}
