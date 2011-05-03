package com.mindplex.common.pool;

import java.io.Closeable;

/**
 * A connection that can be used in conjunction with a {@code ConnectionFactory}
 * and {@code ConnectionPool}.  In essence this connection is a wrapper for an
 * underlying connection of type T.  Through this connection object, the target
 * connection can be acquired, closed, and verified to be valid. 
 *
 * @author Abel Perez
 */
public interface Connection<T> extends Closeable
{
    /**
     * Checks if this connection is valid.  Valid in this context can be as
     * simple as checking if the underlying connection is open.
     * 
     * @return <tt>true</tt> if this connection is valid; otherwise <tt>false</tt>.
     */
    public boolean isValid();

    /**
     * Gets the underlying connection wrapped by this connection. Multiple
     * calls to this method always return the same underlying connection.
     * If the {@code close} method on this connection has been called, then
     * this method will returned a closed connection.
     * 
     * @return the underlying connection wrapped by this connection.
     */
    public T get();

    /**
     * Closes this connection.  In other words the underlying connection is
     * closed and will no longer be able to communicate with its target
     * endpoint.
     */
    public void close();
}
