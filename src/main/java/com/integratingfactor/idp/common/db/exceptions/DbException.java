package com.integratingfactor.idp.common.db.exceptions;

public abstract class DbException extends Throwable {
    /**
     * 
     */
    private static final long serialVersionUID = 5192025424512919299L;
    private String error;

    protected DbException(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }

    @Override
    public String getMessage() {
        return error;
    }
}
