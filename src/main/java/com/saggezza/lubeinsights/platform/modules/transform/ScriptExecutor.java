package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import javax.script.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by venkateswararao on 11/10/14.
 */

/**
 *
 */

public class ScriptExecutor implements Function {

    private String script;
    private boolean isCompile = true;
    private Selection selection;
    private List<String> userDefinedBeans;
    private static final String DATA_ELEMENT = "dataElement";

    private transient ScriptEngine scriptEngine;
    private transient CompiledScript compiledScript;

    public ScriptExecutor() {
        userDefinedBeans = new ArrayList<>();
        script = StringUtils.EMPTY;
    }

    public ScriptExecutor(Params params) {
        script = params.getFirst();
        isCompile = params.getSecond();
        selection = params.size() > 2 ? params.getThird() : null;
        userDefinedBeans = params.size() > 3 ? params.get(3) : new ArrayList<>();
    }

    @Override
    public DataElement apply(DataElement record) {
        StringBuilder updatedScript = new StringBuilder();
        updatedScript.append("with(imports){").append(script).append("}");
        if (selection != null) {
            record = record.select(selection);
        }
        try {
            initializeScriptEngine(record);
            if (isCompile) {
                initializeCompiledScript(updatedScript.toString());
                if (compiledScript != null) {
                    compiledScript.eval();
                } else {
                    throw new RuntimeException("Engine can't compile code");
                }
            } else {
                scriptEngine.eval(updatedScript.toString());
            }
            record = (DataElement) scriptEngine.get(DATA_ELEMENT);
        } catch (ScriptException e) {
            throw new RuntimeException("ScriptEngine can't execute code due to ", e);
        }
        return new DataElement(record.allValues());
    }

    private String getBeans() {
        String beansDelimiter = ",";
        StringBuilder updatedScript = new StringBuilder();
        updatedScript.append("var imports = new JavaImporter(");
        if (CollectionUtils.isNotEmpty(userDefinedBeans))
            updatedScript.append(StringUtils.join(userDefinedBeans, beansDelimiter)).append(beansDelimiter);
        updatedScript.append(StringUtils.join(getPredefinedBeans(), beansDelimiter));
        updatedScript.append(");");
        return updatedScript.toString();
    }

    private List<String> getPredefinedBeans() {
        List<String> predefinedBeans = new ArrayList<>();
        predefinedBeans.add("com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement");
        predefinedBeans.add("com.saggezza.lubeinsights.platform.core.common.datamodel.DataType");
        return predefinedBeans;
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        return in;
    }

    public void initializeScriptEngine(DataElement record) throws ScriptException {
        if (scriptEngine == null) {
            ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
            scriptEngine = scriptEngineManager.getEngineByName("nashorn");
            scriptEngine.eval(getBeans());
        }
        scriptEngine.put(DATA_ELEMENT, record);
    }

    public void initializeCompiledScript(String updatedScript) throws ScriptException {
        if (compiledScript == null && scriptEngine instanceof Compilable) {
            Compilable compilable = (Compilable) scriptEngine;
            compiledScript = compilable.compile(updatedScript);
        }
    }

}
