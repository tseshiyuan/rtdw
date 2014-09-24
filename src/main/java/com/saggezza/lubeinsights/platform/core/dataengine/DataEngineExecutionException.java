package com.saggezza.lubeinsights.platform.core.dataengine;

/**
 * @author : Albin
 */
public class DataEngineExecutionException extends Exception{

    private final int errorCode;

    public DataEngineExecutionException(ErrorCode errorCode) {
        this.errorCode = errorCode.getErrorCode();
    }

    public DataEngineExecutionException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode.getErrorCode();
    }

    public DataEngineExecutionException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode.getErrorCode();
    }

    public DataEngineExecutionException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode.getErrorCode();
    }

    public int getErrorCode() {
        return errorCode;
    }
}
