package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by venkateswararao on 26/9/14.
 */
public class Regex implements Predicate {

    private final String regex;
    private Pattern pattern;

    public Regex(String regex) {
        this.regex = regex;
        pattern = Pattern.compile(regex);
    }

    private Regex(Params args) {
        this(args.<String>get(0));
    }

    @Override
    public boolean test(DataElement line) {
        Matcher matcher = pattern.matcher(line.asText());
        return matcher.find();
    }

}
