package com.mindplex.cassandra.connection;

/**
 * A connection pool of Cassandra based connections.  This pool uses a
 * {@code ConnectionFactory} for the creation of new connection objects.
 *
 * TODO: add pool exhausted exception to the get method.
 * 
 * @author Abel Perez
 */
public interface ConnectionPool<T>
{
    /**
     * Gets the next available connection from this pool. If no idle connection
     * is available then this method will block for the max wait time defined
     * when this pool was constructed.  The max wait time when this pool is
     * exhausted can be forever or for a fixed time. Should the max wait time
     * be exceeded, this method will throw an exception indicating that this
     * connection pool has been exhausted.
     *
     * @return the next available connection from this pool.
     */
    public T get();

    /**
     * Returns the specified connection back to this pool for reuse.  If the
     * specified connection is not valid, then it will be discarded and a new
     * connection will be created to supplement this pool for the loss of the
     * broken connection.
     *
     * @param connection the connection to release.
     *
     * @return <tt>true</tt> if the connection is releases; otherwise <tt>false</tt>.
     */
    public boolean release(T connection);

    /**
     * Completely removes the specified connection from this pool.
     *
     * @param connection the connection to remove from this pool.
     */
    public void remove(T connection);
}
