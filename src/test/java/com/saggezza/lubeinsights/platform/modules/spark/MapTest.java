package com.saggezza.lubeinsights.platform.modules.spark;

import com.google.common.collect.Lists;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
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

public class MapTest {


    @Before
    public void setUp(){
        URL resource = this.getClass().getResource("/service.conf");
        String file = resource.getFile();
        System.setProperty("service.conf", file);
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
    public void testInvokerTransform() throws DataEngineExecutionException {
        SparkExecutionContext context = sparkExecutionContext();

        Params params = Params.of("testIn", "testOut", "InvokerTransform", "substring", 5, 7);
        ServiceRequest.ServiceStep step = new ServiceRequest.ServiceStep(ServiceCommand.Map,
                params
        );

        new Map(params).execute(step, context);

        List<DataElement> testOut = context.getDataRef("testOut").collect();

        assertThat(testOut.size(), is(2));
        assertThat(testOut.get(0).asText(), is("67"));
        assertThat(testOut.get(1).asText(), is("54"));

    }

    private ArrayList<DataElement> data(){
        return Lists.newArrayList(new DataElement(DataType.TEXT, "1234567890"),
                new DataElement(DataType.TEXT, "0987654321"));
    }

}