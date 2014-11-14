package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by chiyao on 10/7/14.
 */
public class StorageUtil {

    public static final String getTableName(String tenant, String application, String dataStoreName) {
        return tenant+"_"+application+"_"+dataStoreName;
    }

    /**
     * generate an upsert statement
     * @param table
     * @param fields
     * @return
     */
    public static final String genSqlUpsert(String table, String[] fields) {
        StringBuilder sb = new StringBuilder("upsert into ")
                .append(table).append(" (").append(StringUtils.join(fields, ","))
                .append(") values (");
        for (int i=0; i<fields.length;i++) {
            sb.append("?,");
        }
        sb.setLength(sb.length()-1);
        sb.append(")");
        return sb.toString();
    }

    public static final String genSqlUpsertLog(String table, String[] fields) {
        // add a batchid field
        String[] newFields = new String[fields.length+1];
        System.arraycopy(fields,0,newFields,0,fields.length);
        newFields[fields.length]="batchid";
        return genSqlUpsert(table+"_log",newFields);
    }

    /**
     * generate an upsert statement for derived data store
     * Each aggField expands to 5 stats sub-fields
     * @param table
     * @param groupByKeys
     * @param aggFields
     * @return
     */
    public static final String genSqlUpsert(String table, String[] groupByKeys, String[] aggFields, DataModel dataModel) {
        StringBuilder sb = new StringBuilder("upsert into ")
                .append(table).append(" (").append(StringUtils.join(groupByKeys, ",")).append(",");
        int numFields = groupByKeys.length;
        for (int i=0; i<aggFields.length;i++) {
           sb.append(aggFields[i]).append("_").append(Stats.COUNT.name()).append(",");
            if (dataModel.getField(new FieldAddress(aggFields[i])).getMap().size()>1) {
                sb.append(aggFields[i]).append("_").append(Stats.MIN.name()).append(",")
                  .append(aggFields[i]).append("_").append(Stats.MAX.name()).append(",")
                  .append(aggFields[i]).append("_").append(Stats.SUM.name()).append(",")
                  .append(aggFields[i]).append("_").append(Stats.SQSUM.name()).append(",");
                numFields += 5;
            }
            else {
                numFields++;
            }
        }
        sb.append("batchid,flushid) values (");
        for (int i=0; i<numFields;i++) {
            sb.append("?,");
        }
        sb.append("?,?)"); // for batchId and flushid
        return sb.toString();
    }


    /**
     * e.g. create table if not exists us_population (col1 varchar, col2 varchar, col3 varchar constraint pk primary key (col1,col2));
     * @param table
     * @param indexFields
     * @param regularFields
     * @param dataModel
     * @return
     */
    public static final String genSqlCreateTable(String table, String[] indexFields, String[] regularFields, DataModel dataModel) {

        /**
         * example:
         * create table if not exists us_population
         * (col1 varchar, col2 varchar, col3 varchar constraint pk primary key (col1,col2));
         */
        StringBuilder sb = new StringBuilder("create table if not exists ").append(table).append(" (");

        System.out.println(dataModel.toString());

        for (String f: indexFields) {
            sb.append(f).append(" ").append(modelType2sqlType(dataModel.typeAt(f))).append(" not null,");
        }
        if (regularFields != null) {
            for (String f: regularFields) {
                //System.out.println(f);
                sb.append(renameStatsField(f)).append(" ").append(modelType2sqlType(dataModel.typeAt(new FieldAddress(f)))).append(",");
            }
        }
        sb.setLength(sb.length()-1);
        sb.append(" constraint pk primary key (").append(StringUtils.join(indexFields,",")).append(")");
        sb.append(")"); // end of create table
        return sb.toString();
    }

    public static final String genSqlCreateLogTable(String table, String[] indexFields, String[] regularFields, DataModel dataModel) {
        // add batchId to indexFields
        String[] newIndexFields = new String[indexFields.length+1];
        System.arraycopy(indexFields,0,newIndexFields,0,indexFields.length);
        newIndexFields[indexFields.length]="batchid";
        // add batchid to dataModel
        LinkedHashMap<String,DataModel> map = new LinkedHashMap<String,DataModel>(dataModel.getMap());
        map.put("batchid",new DataModel(DataType.TEXT));
        return genSqlCreateTable(table+"_log",newIndexFields,regularFields,new DataModel(map));
    }

    /**
     *  rename x.COUNT to x_COUNT, x.MIN to x_MIN, etc.
     */
    private static final String renameStatsField(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot<0) {
            return field;
        }
        return field.replaceAll("\\.COUNT","_COUNT")
            .replaceAll("\\.MIN","_MIN")
            .replaceAll("\\.MAX","_MAX")
            .replaceAll("\\.SUM","_SUM")
            .replaceAll("\\.SQSUM","_SQSUM");
    }

    public static final String genSqlCreateAggTable(String table, String[] groupByKeys, String[] aggFields, DataModel dataModel) {
        ArrayList<String> regularFields = new ArrayList<String>();
        for (int i=0; i<aggFields.length; i++) {
            regularFields.add(aggFields[i]+"."+Stats.COUNT.name());
            if (dataModel.getField(new FieldAddress(aggFields[i])).getMap().size()>1) { // aggFields[i] is NUMBER
                regularFields.add(aggFields[i]+"."+Stats.MIN.name());
                regularFields.add(aggFields[i]+"."+Stats.MAX.name());
                regularFields.add(aggFields[i] + "." + Stats.SUM.name());
                regularFields.add(aggFields[i]+"."+Stats.SQSUM.name());
            }
        }
        // primary key is groupByKeys + batchid + flushid
        String[] primaryKeys = new String[groupByKeys.length+2];
        System.arraycopy(groupByKeys,0,primaryKeys,0,groupByKeys.length);
        primaryKeys[primaryKeys.length-2] = "batchid";
        primaryKeys[primaryKeys.length-1] = "flushid";
        return genSqlCreateTable(table, primaryKeys, (String[])regularFields.toArray(new String[0]), dataModel);
    }

    /**
     * generate a sql query with group by, aggregate and filter
     * @param tableName
     * @param groupByFields
     * @param aggFields      in form of "max(fieldName) aliasName"
     * @param whereClause
     * @return
     */
    public static final String genSqlQuery(String tableName, String[] groupByFields, String[] aggFields,
                                           String whereClause, String havingClause) {
        StringBuilder sb = new StringBuilder("select ");
        for (String gb: groupByFields) {
            sb.append(gb).append(",");
        }
        if (aggFields != null) {
            for (String af : aggFields) {
                String[] field = af.split(" ");
                sb.append(renameStatsField(field[0])).append(" ").append(field[1]).append(",");
            }
        }
        sb.setLength(sb.length()-1);
        sb.append(" from ").append(tableName);
        if (whereClause != null) {
            sb.append(" where ").append(whereClause);
        }
        sb.append(" group by ");
        for (String gb: groupByFields) {
            sb.append(gb).append(",");
        }
        sb.setLength(sb.length()-1);
        if (havingClause != null) {
            sb.append(" having "+renameStatsField(havingClause));
        }
        return sb.toString();
    }

    /**
     * generate a sql query with filter clause
     * @param tableName
     * @param fields
     * @param whereClause
     * @return
     */
    public static final String genSqlQuery(String tableName, String[] fields, String whereClause) {
        StringBuilder sb = new StringBuilder("select ");
        if(null == fields || fields.length == 0){
            sb.append(" * ");//All fields
        }else{
            for (String f: fields) {
                sb.append(renameStatsField(f)).append(",");
            }
        }
        sb.setLength(sb.length()-1);
        sb.append(" from ").append(tableName);
        if (whereClause != null) {
            sb.append(" where ").append(whereClause);
        }
        return sb.toString();
    }

    public static final HashMap<String,Boolean> genFieldTypes(DataModel dataModel, String[] fields) {
        HashMap<String,Boolean> hm = new HashMap<String,Boolean>();
        if(fields == null){
            Map<String, DataModel> map = dataModel.getMap();//TODO - verify assumption with Chi : All fields with primitive types will be fetched
            for(Map.Entry<String, DataModel> entry : map.entrySet()){
                if(entry.getValue().isPrimitive()){//TODO - verify with Chi, based on below question see if this check is needed.
                    hm.put(entry.getKey(), entry.getValue().getDataType() == DataType.TEXT);
                }
            }
        }else{
            for (String f: fields) {
                //TODO - Verify with Chi, Do we really need field address here, since when we write to store we assume all elements are primitive.
                hm.put(renameStatsField(f),(dataModel.getField(new FieldAddress(f)).getDataType()==DataType.TEXT)); // true iff field is text
            }
        }
        return hm;
    }

        /**
         * convert DataType to phoenix data type
         * @param dataType
         * @return
         */
    public static final String modelType2sqlType(DataType dataType) {
        switch (dataType) {
            case NUMBER: return "DOUBLE";
            case TEXT: return "VARCHAR";
            case DATETIME: return "DATE";
            default: return null; // null maps to null
        }
    }

    /**
     * max(age) -> max(age.MAX), avg(age) -> sum(age.SUM)/sum(avg.COUNT)
     * @param exp
     * @return null if not in the specified form
     */
    public static final String transformAggExpression(String exp) {
        String[] expAlias = exp.toLowerCase().split(" ");
        // exp is expression
        // expAlias[1] is alias
        if (exp.startsWith("count(") && exp.endsWith(")")) {
            return "sum("+exp.substring(4, exp.length() - 1)+".COUNT)";
        }
        if (exp.startsWith("min(") && exp.endsWith(")")) {
            return "min("+exp.substring(4, exp.length() - 1)+".MIN)";
        }
        if (exp.startsWith("max(") && exp.endsWith(")")) {
            return "max("+exp.substring(4, exp.length() - 1)+".MAX)";
        }
        if (exp.startsWith("sum(") && exp.endsWith(")")) {
            return "sum("+exp.substring(4, exp.length() - 1)+".SUM)";
        }
        if (exp.startsWith("avg(") && exp.endsWith(")")) {
            String s = exp.substring(4, exp.length() - 1);
            return String.format("sum(%s.SUM)/sum(%s.COUNT)", s,s);
        }
        // TODO: will do variance and standard deviation when needed
        return exp;
    }


    /**
     * read from a flat file of line records as described in dataModel
     * @param filePath
     * @param dataModel
     * @return
     * @throws java.io.FileNotFoundException
     * @throws java.io.IOException
     */
    public static final ArrayList<DataElement> readFromFile(String filePath, DataModel dataModel) throws FileNotFoundException,IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            Map<String,DataModel> hm = dataModel.getMap();
            int count = 0;
            String[] column = new String[hm.size()];
            DataType[] type = new DataType[hm.size()];
            for (String key: hm.keySet()) {
                column[count] = key;
                type[count++] = hm.get(key).getDataType();
            }
            ArrayList<DataElement> result = new ArrayList<DataElement>();
            for (String line = reader.readLine(); line != null; line = reader.readLine() ) {
                if (line.startsWith("#") || line.isEmpty()) {
                    continue;
                }
                String[] fields = line.split(",");
                // construct data element
                LinkedHashMap rec = new LinkedHashMap<String, DataElement>();
                for (int i = 0; i < fields.length; i++) {
                    switch (type[i]) {
                        case TEXT:
                            rec.put(column[i], new DataElement(DataType.TEXT, fields[i].trim()));
                            break;
                        case NUMBER:
                            rec.put(column[i], new DataElement(DataType.NUMBER, Integer.valueOf(fields[i].trim())));
                            break;
                        case DATETIME:
                            rec.put(column[i], new DataElement(DataType.DATETIME, Utils.CurrentDateSecond.parse(fields[i].trim())));
                            break;
                    }
                }
                result.add(new DataElement(rec));
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


}
