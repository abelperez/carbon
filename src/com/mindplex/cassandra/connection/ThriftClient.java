/**
 * Copyright (C) 2011 Mindplex Media, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.mindplex.cassandra.connection;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TBinaryProtocol;

import org.apache.log4j.Logger;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * TODO: add connection timeout
 * TODO: option for setting framed transport
 *
 * @author Abel Perez
 */
public class ThriftClient //implements Connection<T>
{
    /**
     * The default logger for this Cassandra connection.
     */
    private static final Logger logger = Logger.getLogger(ThriftClient.class.getName());

    /**
     * The default Cassandra host this connection should point to if no
     * host is defined.
     */
    private static final String DEFAULT_HOST = "localhost";

    /**
     * The default Cassandra port this connection should point to if no
     * port is defined.
     */
    private static final int DEFAULT_PORT = 9160;

    /**
     * The Cassandra host this connection points to.
     */
    private String host;

    /**
     * The Cassandra port this connection points to.
     */
    private int port;

    /**
     * The keyspace this connection is associated with. 
     */
    private String keyspace;

    /**
     * The underlying thrift transport for this connection.
     */
    private TTransport transport;

    /**
     * The Cassandra thrift client this connection wraps.
     */
    private Cassandra.Client client;

    /**
     * Constructs this connection with the specified keyspace and
     * the default host and port this connection should point to.
     * 
     * @param keyspace the keyspace this connection is associated with.
     */
    private ThriftClient(String keyspace) {
        this(DEFAULT_HOST, DEFAULT_PORT, keyspace);
    }

    /**
     * Constructs a new connection with the specified host, port, and
     * keyspace this connection should point to.
     *
     * @param host the host this connection points to.
     * @param port the port this connection  points to.
     * @param keyspace the keyspace this connection is associated with.
     */
    private ThriftClient(String host, int port, String keyspace) {

        if (host == null || "".equals(host)) {
            throw new IllegalArgumentException("host cannot be empty: " + host);
        }
        if (port <= 0) {
            throw new IllegalArgumentException("port is invalid: " + port);
        }
        if (keyspace == null || "".equals(host)) {
            throw new IllegalArgumentException("keyspace cannot be empty: " + keyspace);
        }

        this.host = host;
        this.port = port;
        this.keyspace = keyspace;
    }
    
    /**
     * Creates a new connection with the specified keyspace and
     * the default host and port this connection should point to.
     *
     * @param keyspace the keyspace this connection is associated with.
     *
     * @return connection based on the specified parameters.
     */
    public static ThriftClient getInstance(String keyspace) {
        return new ThriftClient(DEFAULT_HOST, DEFAULT_PORT, keyspace);
    }

    /**
     * Creates a new connection with the specified host, port, and
     * keyspace this connection should point to.
     *
     * @param host the host this connection points to.
     * @param port the port this connection  points to.
     * @param keyspace the keyspace this connection is associated with.
     * 
     * @return connection based on the specified parameters.
     */
    public static ThriftClient getInstance(String host, int port, String keyspace) {
        return new ThriftClient(host, port, keyspace);
    }

    /**
     * Checks if this connection is open by verifying that the underlying
     * client wrapped by this connection is valid and the thrift transport
     * is open.
     * 
     * @return <tt>true</tt> if this connection is open; otherwise <tt>false</tt>.
     */
    public boolean isOpen() {
        if (transport == null) return false;
        return transport.isOpen();
    }

    /**
     * Opens this connection for communication with the Cassandra node that
     * this connection is associated with.  This method can return false, if
     * no connection is established or the connection is already open.  
     * 
     * @return <tt>true</tt> if the connection is opened; otherwise <tt>false</tt>.
     *
     * @throws ConnectionException can occur if a connection cannot
     * be established with the Cassandra node this connection is associated with.
     */
    public boolean open() throws ConnectionException {
        
        // no need to continue if connection is already open.
        if (isOpen()) {
            return false;    
        }

        try {
            // create and open new framed transport.
            transport = new TFramedTransport(new TSocket(host, port));
            transport.open();

        } catch (Exception exception) {
            String message = "Failed to open connection. ["+ host + ":" + port + "]";
            logger.error(message, exception);
            throw new ConnectionException(message, exception);
        }
        
        return true;
    }

    /**
     * Closes this connection by flushing and closing the underlying thrift
     * transport this connection wraps.  If the connection cannot be closed
     * this method will return false.
     * 
     * @return <tt>true</tt> if the connection is closed; otherwise <tt>false</tt>.
     */
    public boolean close() {
        boolean closed = true;
        try {
            // if this connection is open, attempt to close it;
            // otherwise bail.
            if (isOpen()) {
                transport.flush();
            }

        } catch (Exception exception) {
            logger.error("Failed to flush transport.", exception);
            
        } finally {
            try {
                transport.close();
                // if we failed to flush the transport but successfully
                // closed it, then we can return true.
                closed = true;
                
            } catch (Exception exception) {
                logger.error("Failed to close transport.", exception);
                closed = false;
            }
        }

        return closed;
    }

    /**
     * Gets the thrift Cassandra client this connection wraps. If this
     * connection is not open, this method will ensure the connection
     * gets opened before returning the Cassandra client.
     *
     * @return the thrift based Cassandra client this connection wraps.
     */
    public Cassandra.Client getClient() {
        
        // ensure this connections transport is open.
        // By calling open, we simply force this connection
        // to be opened if it's currently closed. If this
        // connection is already open nothing happens as the
        // result of calling open.
        open();

        // if this is the first time this operation is called we construct
        // and return a new Cassandra client connection.
        if (client == null) {
            client = new Cassandra.Client(new TBinaryProtocol(transport));
            try {
                // set the keyspace this connection is associated with.
                client.set_keyspace(keyspace);

            } catch (Exception exception) {
                logger.error("Failed to set keyspace: " + keyspace, exception);
                throw new ConnectionException("Failed to set keyspace: " + keyspace, exception);
            }
        }

        return client;
    }
}
