package com.viasat.pipeline;

import org.apache.avro.generic.GenericRecord;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;


public class CSVConsumer extends ConsumerBase {

    private final File outFile;

    public CSVConsumer(String broker, String outFile) {
        super(broker);

        this.outFile = new File(outFile);
        // If the output file doesn't exist yet, create it.
        if (!this.outFile.exists()) {
            try {
                this.outFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This method contains the poll loop, where the consumer receives messages from the Kakfa
     * broker and writes them to CSV.
     */
    @Override
    public void onMessage(long offset, String key, GenericRecord value) {
        try (PrintWriter pw = new PrintWriter(this.outFile)) {
            int basketNum = (int) value.get("BasketNum");
            String date = value.get("Date").toString();
            String productNum = value.get("ProductNum").toString();
            double spend = (double) value.get("Spend");
            int units = (int) value.get("Units");
            String storeR = value.get("StoreR").toString();
            String line = String.format("%d,%s,%s,%f,%d,%s", basketNum,
                    date, productNum, spend, units, storeR);
            System.out.println(line);
            pw.println(line);
            pw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
