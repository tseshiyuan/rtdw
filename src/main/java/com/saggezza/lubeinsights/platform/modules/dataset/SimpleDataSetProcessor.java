package com.saggezza.lubeinsights.platform.modules.dataset;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.Environment;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;

import java.io.*;

/**
 * Created by chiyao on 7/31/14.
 */

/**
 * This is a test module that takes an input DataRef and transforms into an output DataRef
 * Input DataRef can be a FILE or VALUE. Output DataRef must be a FILE
 */
public class SimpleDataSetProcessor implements DataSetModule {
    public void run(Environment environment, DataRef inputDataSet,  DataRef outputDataSet) {
        if (outputDataSet.getType() != DataRefType.FILE) { // works for file output only
            return;
        }
        // just copy whatever input to output but appending each line with *
        switch (inputDataSet.getType()) {
            case VALUE:
                String val = inputDataSet.getValue(); // convert value to string representation
                try (PrintWriter writer = new PrintWriter(outputDataSet.getFileName())) {
                    writer.println(val+"*");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case FILE:
                try (BufferedReader br = new BufferedReader(new FileReader(inputDataSet.getFileName()));
                     PrintWriter writer = new PrintWriter(outputDataSet.getFileName())) {
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        writer.println(line+"*");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            // don't do STORE for now
            default:
        }
    }
}
