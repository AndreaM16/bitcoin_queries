package bitcoin.queries;

import org.apache.log4j.BasicConfigurator;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MainQuery_1 {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        BasicConfigurator.configure();
        // Initalize bitcoinJ
        BriefLogFormatter.init();
        NetworkParameters networkParameters = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());
        long startTime = System.currentTimeMillis();

            // Read the blockchain files from the disk
            List<File> blockChainFiles = new LinkedList<File>();
            for (int i = 0; true; i++) {
                File file = new File(Settings.BLOCKCHAIN_PATH + String.format(Locale.US, "blk%05d.dat", i));
                if (!file.exists())
                    break;
                blockChainFiles.add(file);
            }

            String file_path = Settings.QUERY1_PATH + "/" +"result" + ".txt";
            File f = new File(file_path);
            FileWriter w = new FileWriter(f);

            BlockFileLoader bfl = new BlockFileLoader(networkParameters, blockChainFiles);

            String hash_last_block="0000000000000000228ffe0a981f4e33225eec2d303a8794585bde3d5a6805b0";

            for (Block block : bfl) {

                if (block.getHashAsString().equals(hash_last_block)) {
                    break;
                }
                    for (Transaction t : block.getTransactions()) {
                        try {
                            List<TransactionOutput> s = t.getOutputs();
                            for (TransactionOutput l : s) {
                                if (l.getScriptPubKey().isOpReturn()) {
                                    writeToFile(w, t);
                                }
                            }
                        } catch (Exception e) {
                            //e.printStackTrace();
                        }
                    }
            }
            w.flush();
            w.close();

        System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) / 1000);
    }


    private static void writeToFile(FileWriter file, Transaction t) throws IOException {
        file.append(t.getHashAsString() + "\n");
    }

}
