package com.saggezza.lubeinsights.platform.core.common.modules;

/**
 * @author : Albin
 */
public class ModuleException extends RuntimeException {

    public ModuleException() {
    }

    public ModuleException(String message) {
        super(message);
    }

    public ModuleException(String message, Throwable cause) {
        super(message, cause);
    }

}
