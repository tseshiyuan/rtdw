package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;

/**
 * @author : Albin
 */
public class InvokerTransform implements Function{


    private String method;
    private Object[] arguments;

    InvokerTransform() {
    }

    public InvokerTransform(Params params) {
        method = params.getFirst();
        arguments = params.remainingFrom(1).asList().toArray();
    }

    @Override
    public DataElement apply(DataElement dataElement) {
        Object value = dataElement.value();
        try {
            Method method1 = ReflectionUtils.matchingMethod(value.getClass(), arguments, method);
            if(method1 == null){
                throw new RuntimeException("Does not match any method "+method);
            }
            Object invoke = method1.invoke(value, arguments);
            DataType dataType = DataType.forValue(invoke);
            return new DataElement(dataType, invoke);
        } catch (Exception e) {
            throw new RuntimeException("Dynamic method invocation failed",e);
        }
    }

    private Class<?> classForDataType(DataType dataType){//Patch logic for data model transformation
        switch (dataType){
            case TEXT: return String.class;
            case NUMBER:return Number.class;
            case DATETIME:return Date.class;
            default:throw new RuntimeException("Not a valid data type.");
        }
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        try {
            DataType dataType = in.getDataType();
            Class<?> aClass = classForDataType(dataType);
            Method method1 = ReflectionUtils.matchingMethod(aClass, arguments, method);
            return new DataModel(DataType.forClass(method1.getReturnType()));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
