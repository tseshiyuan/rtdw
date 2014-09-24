package com.saggezza.lubeinsights.platform.modules.dataset;

import com.saggezza.lubeinsights.platform.core.common.Environment;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by chiyao on 7/31/14.
 */

/**
 * This is a module that takes an input DataRef and transforms into an output DataRef whose elements were not seen before
 * Assuming the first field of a data element line is a timestamp
 */
public class DataSetDeduper implements DataSetModule {
    public void run(Environment environment, DataRef inputDataSet,  DataRef outputDataSet) {
        if (outputDataSet.getType() != DataRefType.FILE ||
            inputDataSet.getType() != DataRefType.FILE) { // works for file input and file output only
            return;
        }

        // just copy whatever input to output but appending each line with *
        try (BufferedReader br = new BufferedReader(new FileReader(inputDataSet.getFileName()));
             PrintWriter writer = new PrintWriter(outputDataSet.getFileName())) {
            String line;
            String lastTS = environment.getValue("lastTS");

            while ((line = br.readLine()) != null) {
                String newTS = line.split(",")[0];
                if (lastTS==null || newTS.compareTo(lastTS) > 0) {  //only get lines with timestamp > lastTS
                    environment.setValue("lastTS",newTS );
                    writer.println(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
