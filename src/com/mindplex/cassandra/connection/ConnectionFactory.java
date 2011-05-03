package com.mindplex.cassandra.connection;

import com.mindplex.cassandra.CassandraNode;

/**
 * A connection factory for creating and destroying Cassandra based
 * connections.  
 *
 * @author Abel Perez
 */
public interface ConnectionFactory<T extends Connection<?>>
{
    /**
     * Verifies that this connection factory is in a state were
     * it can create connections.
     *
     * @return <tt>true</tt> if connections can ve created; otherwise <tt>false</tt>.
     */
    public boolean canCreate();

    /**
     * Creates a new connection of type T that defaults to localhost as the
     * target endpoint and is associated with the specified Cassandra keyspace.
     * 
     * @param keyspace the Cassandra keyspace the new connection should be
     * associated with.
     *
     * @return a new Cassandra based connection.
     */
    public T create(String keyspace);

    /**
     * Creates a new connection of type T that points to the specified
     * Cassandra node.
     * 
     * @param node the Cassandra node the new connection points to.
     *
     * @return a new Cassandra based connection.
     */
    public T create(CassandraNode node);

    /**
     * Destroys the specified connection by fully closing the connection.
     *
     * @param connection the connection to destroy.
     */
    public void destroy(T connection);
}
