package com.dhan.ticker.exception;

public class InvalidInstrumentException extends RuntimeException {

    public InvalidInstrumentException(String message) {
        super(message);
    }
}
