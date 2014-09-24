package com.saggezza.lubeinsights.platform.core.workflowengine;

/**
 * Created by chiyao on 7/16/14.
 */
public enum ExecStatus {
    OK("OK"),         // complete and have a return value
    ERROR("ERROR"),
    NOT_RUN(null)     // no need to run due to predecessor error
    ;

    // convert service status code to worlflow engine ExecStatus
    private ExecStatus(String serviceStatus) {
        // TODO: map service status code (TBD) to this enum
        statusCode = serviceStatus;
    }
    private String statusCode;

    public static final ExecStatus fromStatusNode(String statusCode) {
        if (statusCode.equalsIgnoreCase("OK")) {
            return OK;
        }
        else {
            return ERROR;
        }
    }

}
