package com.mindplex.cassandra.connection;

/**
 *
 * @author Abel Perez
 */
public class ConnectionException extends RuntimeException
{
    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(Exception exception) {
        super(exception);
    }

    public ConnectionException(String message, Exception exception) {
        super(message, exception);
    }
}
