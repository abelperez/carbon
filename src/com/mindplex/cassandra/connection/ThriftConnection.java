package com.mindplex.cassandra.connection;

import com.mindplex.cassandra.CassandraNode;

/**
 * A thrift connection that holds a thrift client as its underlying connection.
 * 
 * @author Abel Perez
 */
public class ThriftConnection implements Connection<ThriftClient>
{
    /**
     * The target resource this connection holds.
     */
    private ThriftClient connection;

    /**
     * Constructs this connection with the specified Cassandra keyspace.
     *
     * @param keyspace the keyspace to associate this connection with.
     */
    public ThriftConnection(String keyspace) {
        connection = ThriftClient.getInstance(keyspace);
        connection.open();
    }

    /**
     * Constructs this connection with the specified Cassandra node.
     *
     * @param node the Cassandra node to associate this connection with.
     */
    public ThriftConnection(CassandraNode node) {
        connection = ThriftClient.getInstance(node.getKeyspace());
        connection.open();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isValid() {
        return connection.isOpen();
    }

    /**
     *{@inheritDoc}
     */
    public ThriftClient get() {
        return connection;
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        connection.close();
    }
}
