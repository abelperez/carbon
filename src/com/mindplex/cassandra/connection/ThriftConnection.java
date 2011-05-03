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
