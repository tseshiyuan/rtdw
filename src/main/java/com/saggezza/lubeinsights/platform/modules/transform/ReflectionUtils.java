package com.saggezza.lubeinsights.platform.modules.transform;

import java.lang.reflect.Method;

/**
 * @author : Albin
 */
public class ReflectionUtils {



    public static Method matchingMethod(Class aClass, Object[] arguments, String method){
        Method[] methods = aClass.getMethods();
        for(Method meth : methods){
            Class<?>[] parameterTypes = meth.getParameterTypes();
            if(meth.getName().equals(method)){
                if(parameterTypes.length == arguments.length){
                    for(int i=0; i < parameterTypes.length; i++){
                        if(!parameterTypes[i].isInstance(arguments[i])){
                            continue;
                        }
                    }
                    return meth;
                }
            }
        }
        return null;
    }

}
