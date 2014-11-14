package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;
import com.saggezza.lubeinsights.platform.modules.transform.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author : Albin
 */
public class InvokerPredicate implements Predicate{

    private String method;
    private Object[] arguments;

    InvokerPredicate() {
    }

    public InvokerPredicate(Params params) {
        method = params.getFirst();
        arguments = params.remainingFrom(1).asList().toArray();
    }

    @Override
    public boolean test(DataElement dataElement) {
        Object value = dataElement.value();
        try {
            Method method1 = ReflectionUtils.matchingMethod(value.getClass(), arguments, method);
            Object invoke = method1.invoke(value, arguments);
            return (Boolean) invoke;
        } catch (Exception e) {
            throw new RuntimeException("Dynamic method invocation failed",e);
        }
    }

}
