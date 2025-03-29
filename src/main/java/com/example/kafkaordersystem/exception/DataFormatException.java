package com.example.kafkaordersystem.exception;


/**
 * @Classname NetworkException
 * @Description TODO
 * @Date 2025/3/29 11:38
 * @Author Wilson Chen
 */
public class DataFormatException extends RuntimeException {
    public DataFormatException(String message) {
        super(message);
    }
}
