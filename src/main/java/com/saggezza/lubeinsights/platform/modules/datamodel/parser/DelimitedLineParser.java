package com.saggezza.lubeinsights.platform.modules.datamodel.parser;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.function.BiFunction;

/**
 * Created by chiyao on 8/25/14.
 */
public class DelimitedLineParser implements BiFunction<String,DataModel,DataElement> {

    public static final Logger logger = Logger.getLogger(DelimitedLineParser.class);

    /**
     *
     * @param line
     * @param dataModel
     * @return a data element by parsing line for dataModel, which describes each comma delimited field in line
     */
    public final DataElement apply(String line, DataModel dataModel) {
        String[] fields = line.split(",");
        if (!dataModel.isList() || dataModel.getList().size() != fields.length) {
            logger.trace("Can't apply DelimitedLineParser to data model "+dataModel.toJson());
            return null;
        }
        ArrayList<DataElement> el = new ArrayList<DataElement>();
        ArrayList<DataModel> ml = dataModel.getList();
        for (int i=0; i<fields.length; i++) {
            if (!ml.get(i).isPrimitive()) {
                logger.trace("Can't apply DelimitedLineParser to data model "+dataModel.toJson());
                return null;
            }
            switch (ml.get(i).getDataType()) {
                case TEXT:
                    el.add(new DataElement(DataType.TEXT, fields[i]));
                    break;
                case NUMBER:
                    el.add(new DataElement(DataType.NUMBER, fields[i]));
                    break;
                case DATETIME:
                    el.add(new DataElement(DataType.DATETIME, fields[i]));
                    break;
            }
        }
        return new DataElement(el);
    }



}
