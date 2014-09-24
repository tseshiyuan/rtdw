package com.saggezza.lubeinsights.platform.core.common.datamodel;

/**
 * Created by chiyao on 8/21/14.
 */

import com.saggezza.lubeinsights.platform.core.common.Utils;

import java.text.ParseException;
import java.util.Date;

/**
 * There are 3 primitive dataaccess types. Structural dataaccess type can either be an array of name value map (similar to json)
 */
public enum DataType {
    TEXT(1) {
        @Override
        public Object deserialize(String data) {
            return data;
        }

        @Override
        public String serialize(Object data) {
            return data.toString();
        }
    },
    NUMBER(2) {
        @Override
        public Object deserialize(String data) {
            return Double.parseDouble(data);
        }

        @Override
        public String serialize(Object data) {
            Number number = (Number) data;
            return number.doubleValue()+"";

        }
    },
    DATETIME(3) {
        @Override
        public Object deserialize(String data) {
            try {
                return Utils.CurrentDateSecond.parse(data);
            } catch (ParseException e) {
                throw new RuntimeException(e);
                //TODO - Come up with error strategies.
            }
        }

        @Override
        public String serialize(Object data) {
            Date date = (Date) data;
            return Utils.CurrentDateSecond.format(date);
        }
    };

    private final int ordinal;

    DataType(int ordinal) {
        this.ordinal = ordinal;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public static DataType forValue(Object object){
        if(object instanceof Number){//TODO - extend the date time format.
            return NUMBER;
        }
        return TEXT;
    }

    public abstract Object deserialize(String data);

    public abstract String serialize(Object data);

    public static DataType ordinal(int ordinal) {
        for(DataType each : values()){
            if(each.getOrdinal() == ordinal){
                return each;
            }
        }
        return null;
    }
}
