package bitcoin.queries;

import org.apache.log4j.BasicConfigurator;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.*;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class Query_4 {

    public static void main(String[] args) throws IOException {

        BasicConfigurator.configure();
        // Initalize bitcoinJ
        BriefLogFormatter.init();
        NetworkParameters networkParameters = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());

        // Read the blockchain files from the disk
        List<File> blockChainFiles = new LinkedList<File>();
        for (int i = 0; true; i++) {
            File file = new File(Settings.BLOCKCHAIN_PATH + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            blockChainFiles.add(file);
        }

        long startTime = System.currentTimeMillis();
         HashMap<Address,Coin> addresses_coin= new HashMap<>();

        BlockFileLoader bfl = new BlockFileLoader(networkParameters, blockChainFiles);

        // Iterate over the blocks in the blockchain.
        Coin limit = Coin.parseCoin("100.00");
        LocalDateTime end_date = LocalDateTime.of(2012, Month.SEPTEMBER, 22, 10, 47, 52);

        for (Block block : bfl) {

            if (block.getTime().toInstant().isAfter(end_date.toInstant(ZoneOffset.UTC))) {
                System.out.println(block.getTime());
                break;
            }

            for (Transaction t : block.getTransactions()) {
                try {
                    List<TransactionOutput> s = t.getOutputs();
                    for (TransactionOutput l : s) {
                        Coin currTCoins = l.getValue();
                        Script script = l.getScriptPubKey();
                        Address address = script.getToAddress(networkParameters);

                        if (!addresses_coin.containsKey(address)) {
                            addresses_coin.put(address, currTCoins);
                        } else {
                            Coin temp = addresses_coin.get(address);
                            addresses_coin.put(address, currTCoins.add(temp));
                        }
                    }
                }
                catch(Exception e){
                    //e.printStackTrace();
                }
            }

        }

        for(Iterator<Map.Entry<Address, Coin>> it = addresses_coin.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Address, Coin> entry = it.next();
            if(entry.getValue().isLessThan(limit)) it.remove();
        }


        HashMap<Address,Coin> result_map= getMapSortedByListSize(addresses_coin);
        save_addressToFile(result_map,addresses_coin.size());
        System.out.println("Elapsed time: " + (System.currentTimeMillis()-startTime)/1000);
    }

    private static HashMap<Address, Coin> getMapSortedByListSize(HashMap<Address, Coin> map) {
        return map.entrySet().stream()
                .sorted(Collections.reverseOrder((e1, e2) -> e1.getValue().compareTo(e2.getValue())))
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

    private static void save_addressToFile(HashMap<Address,Coin> map,int num_address) throws IOException {
        File f = new File(Settings.QUERY4_PATH + "query_4.txt");
        FileWriter w = new FileWriter(f);
        for (HashMap.Entry<Address, Coin> entry : map.entrySet())
        {

            w.append("address: "+entry.getKey() + "  number of coins: "+ (entry.getValue().getValue()/100000000)+"\n");
        }
        w.append("number of addresses: " + num_address);
        w.flush();
        w.close();
    }

}
