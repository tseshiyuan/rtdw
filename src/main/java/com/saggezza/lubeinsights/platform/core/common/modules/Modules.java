package com.saggezza.lubeinsights.platform.core.common.modules;

import com.saggezza.lubeinsights.platform.core.common.ConditionExpression;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Aggregator;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author : Albin
 */
public class Modules {

    private Modules() {
    }

    public static Predicate predicate(ConditionExpression expression) throws ModuleException {
        return newInstance("predicate", expression.getPredicate(), expression.getParams());
    }

    private static <T> T newInstance(String inPackage, String name, Params args) throws ModuleException {
        try {
            String application = "platform";
            if (name.contains(".")) {
                String[] splitName = name.split("\\.");
                application = splitName[0];
                name = splitName[1];
            }
            StringBuilder sb = new StringBuilder("com.saggezza.lubeinsights.").append(application).append(".modules.").
                    append(inPackage).append(".").append(name);
            Class<?> predicateClass = Class.forName(sb.toString());
            Constructor<?> constructor = predicateClass.getDeclaredConstructor(Params.class);
            constructor.setAccessible(true);
            return (T) constructor.newInstance(new Object[]{args});
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ModuleException("Could not find the module class " + name, e);
        }
    }

    public static Function function(String function, Params args) throws ModuleException {
        return newInstance("transform", function, args);
    }

    public static DataEngineModule module(String moduleType, String moduleName, Params args) throws ModuleException {
        return newInstance(moduleType, moduleName, args);
    }

    public static DataEngineMetaSupport metaModule(String moduleType, String moduleName, Params args) throws ModuleException {
        return newInstance(moduleType, moduleName, args);
    }


    public static Aggregator aggregator(String aggregatorName) throws ModuleException {
        return newInstance("aggregator", aggregatorName, Params.None);
    }
}
