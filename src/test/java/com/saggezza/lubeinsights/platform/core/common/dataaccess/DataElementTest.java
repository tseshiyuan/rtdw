package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.google.common.collect.Maps;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.modules.spark.Map;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class DataElementTest {

    @Test
    public void shouldSerializeAndDeserialize(){
        DataElement element = sampleDataElement();

        String serialized = element.toString();
        System.out.println(serialized);
        DataElement deser = DataElement.fromString(serialized);

        assertTrue(deser.isMap());
        assertThat(deser.asMap().size(), is(3));
        assertTrue(deser.asMap().get("mapD").isMap());
        assertThat(deser.asMap().get("mapD").asMap().size(), is(3));
        assertTrue(deser.asMap().get("mapD").asMap().get("arr1").isList());
        assertThat(deser.asMap().get("mapD").asMap().get("arr1").asList().size(), is(5));
        assertThat(deser.asMap().get("mapD").asMap().get("arr1").asList().get(0).asText(), is("test1"));
        assertThat(deser.asMap().get("mapD").asMap().get("arr1").asList().get(1).asText(), is("test2"));
        assertThat(deser.asMap().get("mapD").asMap().get("arr1").asList().get(2).asText(), is("test3"));
        assertThat(deser.asMap().get("mapD").asMap().get("arr1").asList().get(3).asNumber().doubleValue(), is(5D));
        assertThat(deser.asMap().get("mapD").asMap().keySet().toString(), is("[arr1, arr2, val]"));
        Date actual = deser.asMap().get("mapD").asMap().get("arr1").asList().get(4).asDateTime();
        assertThat(actual.getDate(), is(new Date().getDate()));

    }

    private DataElement sampleDataElement() {
        LinkedHashMap<String, DataElement> data = new LinkedHashMap<>();
        data.put("mapD", getMapElement());
        data.put("arrD", getArrayElement());
        data.put("valD", new DataElement(DataType.NUMBER, new Long(1)));
        return new DataElement(data);
    }

    private DataElement getMapElement(){
        LinkedHashMap<String, DataElement> map = new LinkedHashMap<>();
        map.put("arr1", getArrayElement());
        map.put("arr2", getArrayElement());
        map.put("val", new DataElement(DataType.NUMBER, new Long(1)));
        return new DataElement(map);
    }

    private DataElement getArrayElement() {
        ArrayList<DataElement> list = new ArrayList<>();
        DataElement element = new DataElement(list);
        list.add(new DataElement(DataType.TEXT, "test1"));
        list.add(new DataElement(DataType.TEXT, "test2"));
        list.add(new DataElement(DataType.TEXT, "test3"));
        list.add(new DataElement(DataType.NUMBER, 5));
        list.add(new DataElement(DataType.DATETIME, new Date()));
        return element;
    }


}