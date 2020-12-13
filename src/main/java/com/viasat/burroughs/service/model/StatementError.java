package com.viasat.burroughs.service.model;

import com.google.gson.annotations.SerializedName;

public class StatementError extends StatementResponse {
    @SerializedName("error_code")
    private int errorCode;

    private String message;

    private String[] stackTrace;


    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String[] getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String[] stackTrace) {
        this.stackTrace = stackTrace;
    }

}
