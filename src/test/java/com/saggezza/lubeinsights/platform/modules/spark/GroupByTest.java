package com.saggezza.lubeinsights.platform.modules.spark;

import com.google.common.collect.Lists;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceCommand;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class GroupByTest {

    @Before
    public void setUp(){
        URL resource = this.getClass().getResource("/service.conf");
        String file = resource.getFile();
        System.setProperty("service.conf", file);
    }

    @Test
    public void testGroupByNormal(){
        SparkExecutionContext context = sparkExecutionContext();

        ServiceRequest.ServiceStep step = new ServiceRequest.ServiceStep(ServiceCommand.GroupBy,
                Params.of("testIn","testOut", new Selection(0, 1), new Selection(2,3,4), "Sum", "Max", "Min"));

        GroupBy groupBy = new GroupBy();
        groupBy.execute(step, context);

        JavaRDD testOut = context.getDataRef("testOut");
        List collect = testOut.collect();

        assertThat(collect.size(), is(2));
        assertThat(collect.get(0).toString(), is("k3,k4,12.0,8,3"));
        assertThat(collect.get(1).toString(), is("k1,k2,12.0,8,3"));
    }

    private SparkExecutionContext sparkExecutionContext() {
        SparkConf simpleAPP = new SparkConf().setAppName("DataEngineApp "+ Utils.currentTime()).
                setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(simpleAPP);
        SparkExecutionContext context = new SparkExecutionContext(sc);
        JavaRDD<DataElement> in = sc.parallelize(data());
        context.setDataRef("testIn", in);
        return context;
    }

    @Test
    public void testGroupByWithSelectAndDistinct(){
        SparkExecutionContext context = sparkExecutionContext();

        ServiceRequest.ServiceStep step = new ServiceRequest.ServiceStep(ServiceCommand.GroupBy,
                Params.of("testIn","testOut", new Selection(0, 1)));

        GroupBy groupBy = new GroupBy();
        groupBy.execute(step, context);

        JavaRDD testOut = context.getDataRef("testOut");
        List collect = testOut.collect();

        assertThat(collect.size(), is(2));
        assertThat(collect.get(0).toString(), is("k3,k4"));
        assertThat(collect.get(1).toString(), is("k1,k2"));

    }

    private ArrayList<DataElement> data(){
        return Lists.newArrayList(deArray("k1", "k2", 1, 2, 3),
                deArray("k1", "k2", 4, 5, 6),
                deArray("k1", "k2", 7, 8, 9),
                deArray("k3", "k4", 1, 2, 3),
                deArray("k3", "k4", 4, 5, 6),
                deArray("k3", "k4", 7, 8, 9));
    }

    private DataElement deArray(String key1, String key2, Integer v1, Integer v2, Integer v3){
        ArrayList<DataElement> elems = new ArrayList<>();
        elems.add(new DataElement(DataType.TEXT, key1));
        elems.add(new DataElement(DataType.TEXT, key2));
        elems.add(new DataElement(DataType.NUMBER, v1));
        elems.add(new DataElement(DataType.NUMBER, v2));
        elems.add(new DataElement(DataType.NUMBER, v3));
        return new DataElement(elems);
    }


}