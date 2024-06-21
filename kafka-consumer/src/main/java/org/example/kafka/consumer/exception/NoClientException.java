package org.example.kafka.consumer.exception;

public class NoClientException extends RuntimeException {

    public NoClientException(String message) {
        super(message);
    }
}
