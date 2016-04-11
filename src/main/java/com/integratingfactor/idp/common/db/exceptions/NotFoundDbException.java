package com.integratingfactor.idp.common.db.exceptions;

public class NotFoundDbException extends DbException {

    /**
     * 
     */
    private static final long serialVersionUID = -130562593490261724L;

    public NotFoundDbException(String error) {
        super("Not Found: " + error);
    }

}
