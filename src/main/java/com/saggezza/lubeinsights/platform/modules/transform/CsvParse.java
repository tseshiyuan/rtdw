package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

import java.util.ArrayList;

/**
 * @author : Albin
 * Parses the input by the delim.
 */
public class CsvParse implements Function {

    private String delim;
    private int numberOfColumns;//TODO - validate this since this is metadata

    public CsvParse(){}

    public CsvParse(Params params){
        this.delim = params.getFirst();
        this.numberOfColumns = params.size() > 1 ? params.getSecond() : -1;
    }

    @Override
    public DataElement apply(DataElement line) {
        String[] fields = line.asText().split(delim);
        DataElement[] elts = new DataElement[fields.length];
        for (int i=0; i<fields.length; i++) {
            elts[i] = new DataElement(DataType.TEXT,fields[i]);
        }
        return new DataElement(elts);
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        if(numberOfColumns != -1){
            ArrayList<DataModel> dataModels = new ArrayList<>();
            for(int i=0; i < numberOfColumns; i++){
                dataModels.add(new DataModel(DataType.TEXT));
            }
            return new DataModel(dataModels);
        }else {
            throw new DataEngineExecutionException(ErrorCode.Data_Model_Cannot_Be_Known,
                    "Number of expected columns not specified for CsvParse.");
        }
    }

}
