package com.mindplex.cassandra;

/**
 * A {@code CassandraNode} is a simple representation of a Cassandra Node.
 * This object is a convenience object that encapsulates Host, port and
 * keyspace information about a Cassandra node.
 * 
 * @author Abel Perez
 */
public class CassandraNode
{
    /**
     * The hostname for this Cassandra node.
     */
    private String host;

    /**
     * The port for this Cassandra node.
     */
    private int port;

    /**
     * The Keyspace associated with this Cassandra node.
     */
    private String keyspace;

    /**
     * Gets the hostname for this Cassandra node.
     *
     * @return the hostname for this Cassandra node.
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the hostname for this Cassandra node.
     *
     * @param host the hostname for this Cassandra node.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets the port for this Cassandra node.
     *
     * @return the port for this Cassandra node.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port for this Cassandra node.
     *
     * @param port the port for this Cassandra node.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets the Keyspace associated with this Cassandra node.
     *
     * @return the Keyspace associated with this Cassandra node.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Sets the Keyspace associated with this Cassandra node.
     *
     * @param keyspace the Keyspace associated with this Cassandra node.
     */
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
}
