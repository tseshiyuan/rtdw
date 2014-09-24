package com.saggezza.lubeinsights.platform.core.workflowengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;

/**
 * Created by chiyao on 7/15/14.
 */
public class ExecResult {
    private ExecStatus status; // TBD
    private String message;
    private DataChannel value;

    public ExecResult(ExecStatus status, String message, DataChannel value) {
        this.status = status;
        this.message = message;
        this.value = value;
    }

    public final DataChannel getValue() {
        return value;
    }

    public final boolean isOK() {
        return status==ExecStatus.OK;
    }
}
