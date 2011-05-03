package com.mindplex.cassandra.connection;

import com.mindplex.cassandra.CassandraNode;

/**
 * A thrift connection factory for creating instances of {@code ThriftConnection}.
 * 
 * @author Abel Perez
 */
public class ThriftConnectionFactory implements ConnectionFactory<ThriftConnection>
{
    /**
     *{@inheritDoc}
     */
    public boolean canCreate() {
        return true;
    }

    /**
     *{@inheritDoc}
     */
    public ThriftConnection create(String keyspace) {
        return new ThriftConnection(keyspace);
    }

    /**
     *{@inheritDoc}
     */
    public ThriftConnection create(CassandraNode node) {
        return new ThriftConnection(node.getKeyspace());
    }

    /**
     * {@inheritDoc}
     */
    public void destroy(ThriftConnection connection) {
        connection.close();
    }
}
