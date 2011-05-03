package com.mindplex.cassandra.connection;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.mindplex.cassandra.CassandraNode;

/**
 *
 * @author Abel Perez
 */
public class CassandraConnectionPool<T extends Connection<?>> implements ConnectionPool<T>
{
    /**
     * The default logger for this connection pool. 
     */
    private static final Logger logger = Logger.getLogger(CassandraConnectionPool.class);

    /**
     * The list of connections available in this queue.
     */
    private final ArrayBlockingQueue<T> connections;
    
    /**
     * The default amount of max connections this pool will keep open
     * at any given time.
     */
    private static final int DEFAULT_MAX_CONNECTIONS = 5;

    /**
     * This constant value represents blocking forever in the context of
     * controlling thread access. 
     */
    private static final int BLOCK_FOREVER = -1;

    /**
     * The time interval to wait when polling this pool for a connection
     * while it's exhausted.
     */
    private static final int POLL_INTERVAL = 100;

    /**
     * The Cassandra node that connections in this pool point to.
     */
    private CassandraNode node;

    /**
     * The max time to wait for the next available connection in this pool
     * while the pool is exhausted.
     */
    private final int maxWaitTimeWhenExhausted;

    /**
     * The connection factory this connection pool uses to create new connections.
     */
    private ConnectionFactory<T> factory;
    
    /**
     * Constructs this connection pool with the specified host, port, keyspace
     * pool access fairness, and max wait time when pool is exhausted.
     *
     * connection when this pool is exhausted.
     * @param factory factory for creating connections to store in this pool.
     * @param node the Cassandra node this connection pool is associated with.
     */
    public CassandraConnectionPool(CassandraNode node, ConnectionFactory<T> factory) {
        this(node, BLOCK_FOREVER, factory);
    }

    /**
     * Constructs this connection pool with the specified host, port, keyspace
     * pool access fairness, and max wait time when pool is exhausted.
     *
     * @param maxWaitTimeWhenExhausted the max wait time for the next available
     * connection when this pool is exhausted.
     * @param factory factory for creating connections to store in this pool.
     * @param node the Cassandra node this connection pool is associated with. 
     */
    public CassandraConnectionPool(CassandraNode node, int maxWaitTimeWhenExhausted,
                                    ConnectionFactory<T> factory) {

        this.maxWaitTimeWhenExhausted = maxWaitTimeWhenExhausted;
        this.factory = factory;

        connections = new ArrayBlockingQueue<T>(DEFAULT_MAX_CONNECTIONS);
        for (int i = 0; i < DEFAULT_MAX_CONNECTIONS; i++) {
            T connection = factory.create(node.getKeyspace());
            if (connection.isValid()) {
                connections.add(connection);
            }
        }
    }

    /**
     * Gets the next available connection from this pool of Cassandra
     * connections.  When this pool is exhausted, this operation will block for
     * the total wait time this pool has been configured for.
     * 
     * @return a connection to Cassandra from this pool.
     * 
     * @throws ConnectionException can occur if a connection cannot be
     * acquired.
     */
    public T get() throws ConnectionException {

        T connection = null;

        // If the max time to wait for a connection to become
        // available in the pool is forever, we continuously poll
        // the queue until a connection becomes available.
        
        if (maxWaitTimeWhenExhausted == BLOCK_FOREVER) {
            while (connection == null) {
                try {
                    connection = connections.poll(POLL_INTERVAL, TimeUnit.MILLISECONDS);
                  
                } catch (InterruptedException exception) {
                    logger.error("Interrupted while acquiring connection. [wait-time:"
                            +maxWaitTimeWhenExhausted+"]", exception);
                    break;
                }
            }
        } else {
            // Wait the max time allowable for a connection to
            // become available in the pool; otherwise bail.
            try {
                connection = connections.poll(maxWaitTimeWhenExhausted, TimeUnit.MILLISECONDS);
            } catch (InterruptedException exception) {
                logger.error("Interrupted while acquiring connection. [wait-time:"
                        +maxWaitTimeWhenExhausted+"]", exception);
            }
        }

        // we are in bad shape, lets just throw up on the client.
        if (connection == null) {
            throw new ConnectionException("Failed to acquire connection from pool.");
        }
        
        return connection;
    }

    /**
     * Returns the specified connection back to this pool.  If the specified
     * connection is no longer valid, then a new connection is created and
     * added to this pool.
     * 
     * @param connection the connection to return back to this pool.
     *
     * @return <tt>true</tt> if the connection is successfully returned
     * to this pool; otherwise <tt>false</tt>.
     */
    public boolean release(T connection) {

        // no need to continue if the specified connection is bogus.
        if (connection == null) return false;

        try {
            // if the connection is valid we added back to this pool;
            // otherwise we create a new connection in its place and
            // add it to this pool.

            if (connection.isValid()) {
                return connections.add(connection);
            } else {
                return connections.add(factory.create(node));
            }
            
        } catch (IllegalStateException exception) {
            logger.error("Failed to return connection to pool. [max connections exceeded].");
            connection.close();
            return false;
        }
    }

    /**
     * Gets the max time to when for connections when this pool is exhausted.
     *
     * @return the max time to when for connections when this pool is exhausted.
     */
    public int getMaxWaitTimeWhenExhausted() {
        return maxWaitTimeWhenExhausted;
    }

    /**
     * Removes the specified connection from this pool.
     * 
     * @param connection the connection to remove from this pool.
     */
    public void remove(T connection) {
        if (connection != null) {
            connections.remove(connection);
        }
    }
}
