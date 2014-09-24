package com.saggezza.lubeinsights.platform.core.common.modules;

/**
 * Created by chiyao on 7/24/14.
 */

import com.google.common.collect.ImmutableSet;
import com.saggezza.lubeinsights.platform.modules.dataset.DataSetModule;
import org.apache.log4j.Logger;
import com.google.common.reflect.ClassPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

/**
 * This class manages all modules, including built in ones and user defined ones
 */
public class ModuleFactory {

    public static final Logger logger = Logger.getLogger(ModuleFactory.class);
    public static final String MODULE_HOME = "com.saggezza.lubeinsights.platform.modules";

    /**
     *
     * @param name
     * @return the module object for the given module name
     */
    public static DataSetModule getModule(String moduleType, String name) {
        try {
            // find the class of the given name and instantiate it
            String className = MODULE_HOME + "." + moduleType + "." + name;
            return ((Class<DataSetModule>)Class.forName(className)).newInstance();

        } catch (Exception e) {
            logger.trace("Can't find or predicate class "+name, e);
            return null;
        }
    }

    /**
     *
     * @param packageName
     * @return fully qualified class names under this package recursively,  based on current class loader for this class
     * @throws IOException
     */
    public static final ArrayList<String> getModules(String packageName) throws IOException {
        ClassPath classPath = ClassPath.from(ModuleFactory.class.getClassLoader());
        ImmutableSet<ClassPath.ClassInfo> classes = classPath.getTopLevelClasses(packageName);
        ArrayList<String> result = new ArrayList<String>();
        for (ClassPath.ClassInfo info: classes) {
            result.add(info.getName()); // return fully qualified name of the class
        }
        return result;
    }

    public static final void main(String[] args) {
        try {
            ArrayList<String> modules = getModules("com.saggezza.lubeinsights.platform.modules.aggregator");
            for (String m: modules) {
                System.out.println(m);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
