package com.saggezza.lubeinsights.platform.core.dataengine;

/**
 * @author : Albin
 */
public enum ErrorCode {

    Input_Data_Model_Not_Present(1),
    Stand_Alone_Command(2),
    Data_Model_Cannot_Be_Known(3),
    ModuleNotKnown(4),
    PrimitiveDataElementNotExpected(5),
    JoinKeySizesDifferent(6),
    JoinKeyTypeDifferent(7),
    CannotLoadDataStore(8);
    private final int errorCode;

    private ErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
