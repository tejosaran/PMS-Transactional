package com.pms.transactional.exceptions;

public class InvalidTradeException extends RuntimeException{
      private String errorMessage;

    public InvalidTradeException(String message){
        super(message);
        this.errorMessage = "INSUFFICIENT_QUANTITY";
    }

    public InvalidTradeException(String message, String errorMessage){
        super(message);
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage(){
        return errorMessage;
    }
}
