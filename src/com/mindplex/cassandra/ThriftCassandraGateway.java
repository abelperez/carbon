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

package com.mindplex.cassandra;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.thrift.*;

import com.mindplex.cassandra.connection.*;

/**
 *
 * @author Abel Perez
 */
public class ThriftCassandraGateway implements CassandraGateway
{
    /**
     * Default logger used by this gateway.
     */
    private static final Logger logger =
            Logger.getLogger(ThriftCassandraGateway.class.getName());

    /**
     * The default value of {@link #host} which indicates this node
     * points to a local Cassandra node.
     */
    private static final String DEFAULT_HOST = "localhost";

    /**
     * The default value of {@link #port} which indicates this node
     * points to the default port cassandra nodes listen on.
     */
    private static final int DEFAULT_PORT = 9160;

    /**
     * 
     */
    public static final Charset charset = Charset.forName("UTF-8");

    /** */
    public static final CharsetDecoder decoder = charset.newDecoder();
	
    /** */
    public static final CharsetEncoder encoder = charset.newEncoder();
    
    /** */
    private String keyspace;

    /**
     * The Cassandra node this gateway points to.
     */
    private CassandraNode node;

    /**
     *
     */
    private ConsistencyLevel consistencyLevel;
    
    /**
     * A pool of thrift connections that this gateway uses to communicate
     * with the Cassandra node that this gateway points to.
     */
    private ConnectionPool<ThriftConnection> pool;

    /**
     * Constructs this gateway with the specified keyspace.  This gateway
     * defaults to connecting to a Cassandra node running on localhost
     * and the default port of 9160.
     * 
     * @param keyspace the keyspace this gateway is associated with.
     */
    public ThriftCassandraGateway(String keyspace) {
        this(keyspace, ConsistencyLevel.ONE);
    }

    /**
     * Constructs this gateway with the specified keyspace.  This gateway
     * defaults to connecting to a Cassandra node running on localhost
     * and the default port of 9160.
     *
     * @param keyspace the keyspace this gateway is associated with.
     * @param consistencyLevel the consistency level this gateway should
     * enforce when reading/writing to Cassandra.
     */
    public ThriftCassandraGateway(String keyspace, ConsistencyLevel consistencyLevel) {
        this(DEFAULT_HOST, DEFAULT_PORT, keyspace, consistencyLevel);
    }

    /**
     * Constructs this gateway with the specified host, port and keyspace.
     *
     * @param host the Cassandra host this gateway points to.
     * @param port the Cassandra port this gateway points to.
     * @param keyspace the keyspace this gateway is associated with.
     * @param consistencyLevel the consistency level this gateway should
     * enforce when reading/writing to Cassandra. 
     */    
    public ThriftCassandraGateway(String host, int port, String keyspace, ConsistencyLevel consistencyLevel) {

        // verify that specified parameters are valid.

        if (host == null || "".equals(host)) {
            throw new IllegalArgumentException("host cannot be empty.");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("invalid port specified.");
        }
        if (keyspace == null || "".equals(keyspace)) {
            throw new IllegalArgumentException("keyspace cannot be empty.");
        }
        if (consistencyLevel == null) {
            throw new IllegalArgumentException("ConsistencyLevel cannot be null.");
        }

        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;

        // setup a Cassandra node object that represents
        // the node this gateway communicates with.
        
        CassandraNode node = new CassandraNode();
        node.setHost(host);
        node.setPort(port);
        node.setKeyspace(keyspace);
        this.node = node;

        // setup the connection factory and connection pool this gateway
        // uses to communicate with Cassandra.
        
        ConnectionFactory<ThriftConnection> factory = new ThriftConnectionFactory();
        pool = new CassandraConnectionPool<ThriftConnection>(node, factory);
    }

    /**
     * {@inheritDoc}
     */
    public void delete(final String columnFamily, final String rowid, final String column) throws Exception {

        execute(new CassandraFunction<Cassandra.Client>()
        {
            public void execute(Cassandra.Client client) throws Exception {

                // before we execute the actual delete
                // operation we convert the rowid to a
                // byte buffer build up a column path
                // set the system time as the version
                // and use the predefined consistency level

                client.remove(toByteBuffer(rowid),
                        ThriftUtil.getColumnPath(columnFamily, column),
                        System.currentTimeMillis(),
                        getConsistencyLevel());
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public void deleteAll(final String columnFamily, final String rowid, final Pair[] pairs) throws Exception {

        execute(new CassandraFunction<Cassandra.Client>()
        {
            public void execute(Cassandra.Client client) throws Exception {

                // setup a slice predicate that will contain the keys of
                // the columns we want to delete.

                SlicePredicate predicate = new SlicePredicate();
                List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
                for (Pair pair : pairs) {
                    columns.add(toByteBuffer(pair.getKey()));
                }
                predicate.column_names = columns;

                // setup a deletion object that holds our slice predicate.

                Deletion deletion = new Deletion();
                deletion.predicate = predicate;

                // setup a mutation object that will hold our deletion
                // object and add the mutation to a list of mutations.

                Mutation mutation = new Mutation();
                mutation.deletion = deletion;
                List<Mutation> mutations = new ArrayList<Mutation>();
                mutations.add(mutation);

                // setup the map of list mutations, the key for this map is
                // the column family we want to delete columns from.

                Map<String, List<Mutation>> inner = new HashMap<String, List<Mutation>>();
                inner.put(columnFamily, mutations);

                // setup the outer map that contains the inner map of mutations.
                // the key of this map is the row id that we should delete the
                // list of columns from.
                
                Map<ByteBuffer, Map<String, List<Mutation>>> outer =
                        new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                outer.put(toByteBuffer(rowid), inner);

                // lastly we execute a batch mutation which actually
                // deletes all the specified columns from the given column
                // family for the specified row id.
                
                client.batch_mutate(outer, getConsistencyLevel());
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public String findColumn(final String columnFamily, final String rowid, final String key) throws Exception {

        return executeSelect(new CassandraSelectFunction<Cassandra.Client, String>()
        {
            public String execute(Cassandra.Client client) throws Exception {

                // Before invoking the Get operation we
                // convert the given rowid to a byte buffer,
                // setup the column path and set the consistency
                // level.
                
                ColumnOrSuperColumn response = client.get(
                        toByteBuffer(rowid),
                        ThriftUtil.getColumnPath(columnFamily, key),
                        getConsistencyLevel());
                
                return stringValue(response.column.value);
            }
        });
    }

    /**
     * {@inheritDoc}
     */    
    public List<Pair> findColumnsSliceRange(final String columnFamily, final String rowid) throws Exception {

        return executeSelect(new CassandraSelectFunction<Cassandra.Client, List<Pair>>()
        {
            public List<Pair> execute(Cassandra.Client client) throws Exception {

                // setup a slice predicate with a slice range that
                // basically sets the start and end range to all.
                // In other words our column search range is the entire
                // specified row.
                
                SlicePredicate slicePredicate = ThriftUtil.all();

                // before invoking the get_slice operation
                // we convert the rowid to a byte buffer,
                // setup a column parent with the specified
                // column family, set our slice predicate
                // and specify the consistency level.
                
                List<ColumnOrSuperColumn> response = client.get_slice(
                        toByteBuffer(rowid),
                        new ColumnParent(columnFamily),
                        slicePredicate,
                        getConsistencyLevel());

                // now we translate the response we received
                // from the get_slice operation into a list of
                // key value pairs.  Our final result is a list
                // of Pair objects that represent the key value
                // pairs of each column found in our search.
                
                List<Pair> searchResults = new ArrayList<Pair>();
                for (ColumnOrSuperColumn item : response) {
                    searchResults.add(new Pair(stringValue(item.column.name),
                            stringValue(item.column.value)));
                }

                // return the final search results.
                return searchResults;
            }
        });
    }

    /**
     * {@inheritDoc}
     */    
    public List<Pair> findByKeyRange(final String columnFamily, final List<String> keys) throws Exception {

        return executeSelect(new CassandraSelectFunction<Cassandra.Client, List<Pair>>()
        {
            public List<Pair> execute(Cassandra.Client client) throws Exception {

                // setup a slice predicate with a slice range that
                // basically sets the start and end range to all.
                // In other words our column search range is the entire
                // specified row.

                SlicePredicate slicePredicate = ThriftUtil.all();

                // setup a key range that sets the start and end range
                // to all.
                
                KeyRange keyRange = ThriftUtil.allKeyRange();

                // we invoke the get_range_slices operation
                // with a column parent that contains the specified
                // column family, and we set the slice predicate,
                // key range, and consistency level.
                
                List<KeySlice> response = client.get_range_slices(
                        new ColumnParent(columnFamily),
                        slicePredicate,
                        keyRange,
                        getConsistencyLevel());

                List<Pair> searchResults = new ArrayList<Pair>();

                for (KeySlice slice : response) {

                    // for every key we get the list of columns
                    // and convert the key value pairs into a
                    // instances of Pair and add them to the final
                    // search results list.
                    
                    for (ColumnOrSuperColumn item : slice.getColumns()) {
                        searchResults.add(new Pair(
                                    stringValue(item.getColumn().name),
                                    stringValue(item.getColumn().value),
                                    new String(slice.getKey())));
                    }
                }

                // return the final search results.
                return searchResults;
            }
        });
    }

    /**
     * {@inheritDoc}
     */    
    public List<Pair> findColumns(final String columnFamily, final String rowid, final List<String> keys) throws Exception {

        return executeSelect(new CassandraSelectFunction<Cassandra.Client, List<Pair>>()
        {
            public List<Pair> execute(Cassandra.Client client) throws Exception {

                // setup the list of column keys we want to search for.

                List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
                for (String key : keys) {
                    columns.add(toByteBuffer(key));
                }

                // setup a slice predicate that contains the list
                // of column keys we want to search for.
                
                SlicePredicate slicePredicate = new SlicePredicate();
                slicePredicate.column_names = columns;

                // Invoke the get_slice operation with the given row id
                // converted to a byte buffer, a column parent that
                // contains the column family to search, our slice
                // predicate and the consistency level.
                
                List<ColumnOrSuperColumn> response = client.get_slice(
                        toByteBuffer(rowid),
                        new ColumnParent(columnFamily),
                        slicePredicate,
                        getConsistencyLevel());

                // now we translate the response we received
                // from the get_slice operation into a list of
                // key value pairs.  Our final result is a list
                // of Pair objects that represent the key value
                // pairs of each column found in our search.

                List<Pair> searchResults = new ArrayList<Pair>();
                for (ColumnOrSuperColumn item : response) {
                    searchResults.add(new Pair(
                            stringValue(item.getColumn().name),
                            stringValue(item.getColumn().value)));
                }

                // return the final search results.
                return searchResults;
            }
        });
    }

    /**
     * {@inheritDoc}
     */    
    public void insert(final String columnFamily, final String rowid, final Pair pair) throws Exception {

        execute(new CassandraFunction<Cassandra.Client>()
        {
            public void execute(Cassandra.Client client) throws Exception {

                // We convert the specified rowid into a 
                // byte buffer, define a column parent that contains
                // the specified column family, setup the column we
                // want to insert and set the consistency level.
                // And lastly invoke the actual column insert operation.

                client.insert(toByteBuffer(rowid),
                        new ColumnParent(columnFamily),
                        ThriftUtil.makeColumn(pair),
                        getConsistencyLevel());
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public void insert(String columnFamily, String rowid, Pair[] pairs) throws Exception {

        // for each specified pair we invoke the singular insert method.
        // This method is not atomic, if one insert fails, previous inserts
        // are untouched and we continue to try to insert the remaining list
        // of pairs.
        
        for (Pair pair : pairs) {
            try {
                insert(columnFamily, rowid, pair);
                
            } catch (Exception exception) {

                // log the error and continue trying to
                // insert remaining columns.
                
                logger.log(Level.SEVERE, "Failed to insert [cf: " + columnFamily +
                        ", rowid: " + rowid + ", key: " + pair.getKey(), exception);
            }
        }
    }

    /**
     * {@inheritDoc}
     */    
    public void insertAll(final String columnFamily, final String rowid, final Pair[] pairs) throws Exception {

        execute(new CassandraFunction<Cassandra.Client>()
        {
            public void execute(Cassandra.Client client) throws Exception {

                // for each given pair we setup a mutation and add it
                // to a list of mutations we will use to do a batch
                // mutation operations.
                
                List<Mutation> mutation = new ArrayList<Mutation>();
                for (Pair pair : pairs) {
                    mutation.add(ThriftUtil.getMutation(
                            ThriftUtil.makeColumn(pair)));
                }

                // setup a map with the key being the column family we want
                // to insert columns into and the value being the list of
                // mutations that correspond the actual key value pairs
                // we want to insert.
                
                Map<String, List<Mutation>> mutations = new HashMap<String, List<Mutation>>();
                mutations.put(columnFamily, mutation);

                // setup a map with the key being the row id where we should
                // insert our columns to and the value being our mutation map.
                
                Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap =
                        new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                mutationMap.put(toByteBuffer(rowid), mutations);

                // execute the actual batch mutation.
                client.batch_mutate(mutationMap, getConsistencyLevel());
            }
        });
    }

    /**
     * {@inheritDoc}
     */    
    public void insertAllSuperColumns(final String columnFamily, final String superColumnName, final String rowid, final Pair[] pairs) throws Exception {

        execute(new CassandraFunction<Cassandra.Client>()
        {
            public void execute(Cassandra.Client client) throws Exception {

                //

                SuperColumn superColumn = new SuperColumn();
                superColumn.setName(superColumnName.getBytes());

                // for each given pair we setup a Column and add it
                // to a list of columns we will use to do a batch
                // mutation operation.

                List<Column> columns = new ArrayList<Column>();
                for (Pair pair : pairs) {
                    columns.add(ThriftUtil.makeColumn(pair));
                }
                superColumn.setColumns(columns);


                // setup a list of mutations that contains a mutation
                // with our super column.
                
                List<Mutation> mutation = new ArrayList<Mutation>();
                mutation.add(ThriftUtil.getMutation(superColumn));

                // setup a map with the key being the column family we want
                // to insert columns into and the value being the list of
                // mutations that correspond the actual key value pairs
                // we want to insert.

                Map<String, List<Mutation>> mutations = new HashMap<String, List<Mutation>>();
                mutations.put(columnFamily, mutation);

                // setup a map with the key being the row id where we should
                // insert our columns to and the value being our mutation map.

                Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap =
                        new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                mutationMap.put(toByteBuffer(rowid), mutations);

                // execute the actual batch mutation.
                client.batch_mutate(mutationMap, getConsistencyLevel());
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public void deleteBySliceRange() throws Exception {
        throw new UnsupportedOperationException();
    }
    
    /**
     * {@inheritDoc}
     */    
    public void discover() throws Exception {

        execute(new CassandraFunction<Cassandra.Client>()
        {
            public void execute(Cassandra.Client client) throws Exception {

                for (KsDef def : client.describe_keyspaces()) {

                    if (ThriftUtil.SYSTEM_KEYSPACE.equals(def.getName())) continue;

                    List<TokenRange> tokens = client.describe_ring(def.getName());
                    for (TokenRange range : tokens) {
                        for (String node : range.getEndpoints()) {
                            System.out.println("discovered node: " + node);
                        }
                    }
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */    
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Executes the specified Cassandra function within a Cassandra
     * client session.  In essence this method controls the borrowing
     * and releasing of thrift connections from the thrift connection
     * pool.  This method does not return a value and therefore is
     * recommended as an insert, update, delete type of function
     * executor.  For operations that require a return value use
     * {@code executeSelect}.
     *
     * The following is an example of the intended usage for this
     * method.  This example illustrates the bare minimum way to
     * use this method with a thrift client.
     *
     * <pre>
     * {@code
     * class ExampleFunction implements CassandraFunction<Cassandra.Client>
     * {
     *     public void execute(Cassandra.Client client) throws Exception {
     *         // do something with client... 
     *     }
     * }}
     * </pre>
     *
     * execute(new ExampleFunction());
     * </code>
     *
     * @param function the cassandra function to execute.
     *
     * @throws Exception can occur if the specified function fails.
     */
    public void execute(CassandraFunction<Cassandra.Client> function) throws Exception {

        // get a thrift connection from the connection pool.
        ThriftConnection connection = pool.get();

        try {
            // execute the specified callback function within
            // a thrift client session.
            
            function.execute(connection.get().getClient());

        } catch (Exception exception) {
            logger.log(Level.SEVERE, "Failed to execute cassandra function.", exception);
            throw new Exception("Failed to execute cassandra function.", exception);

        } finally {

            // make sure we release our connection back to the
            // connection pool.

            pool.release(connection);
        }
    }

    /**
     * Executes the specified Cassandra function within a Cassandra
     * client session.  In essence this method controls the borrowing
     * and releasing of thrift connections from the thrift connection
     * pool.  This method return a value and therefore is recommended
     * for search type of functions. See {@code execute} for example
     * usage.
     *
     * @param function the cassandra function to execute.
     *
     * @return the result of executing the specified function.
     *
     * @throws Exception can occur if the specified function fails.
     */
    public <T> T executeSelect(CassandraSelectFunction<Cassandra.Client, T> function) throws Exception {

        // get a thrift connection from the connection pool.
        ThriftConnection connection = pool.get();

        try {
            // execute the specified callback function within
            // a thrift client session.

            return function.execute(connection.get().getClient());

        } catch (Exception exception) {
            logger.log(Level.SEVERE, "Failed to execute cassandra select function.", exception);
            throw new Exception("Failed to execute cassandra select function.", exception);

        } finally {

            // make sure we release our connection back to the
            // connection pool.

            pool.release(connection);
        }
    }

    /**
     * Converts the specified string value into a byte buffer.  This method
     * is a convenience method heavily used by this gateway, since Cassandra
     * stores its key values as binary values.
     *
     * @param value the string value to convert into a byte buffer.
     *
     * @return a byte buffer representation of the specified string.
     */
    public ByteBuffer toByteBuffer(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

    /**
     * Converts the specified byte buffer into a string value.  This method
     * is a convenience method heavily used by this gateway, since Cassandra
     * stores its key values as binary values.
     *
     * @param buffer the byte buffer to convert into a string value.
     *
     * @return a string representation of the specified byte buffer.
     *
     * @throws Exception can occur during the byte buffer conversion to string.
     */
    public String stringValue(ByteBuffer buffer) throws Exception {

        int position = buffer.position();
        String data = decoder.decode(buffer).toString();

        // reset buffer's position to its original so
        // it's not altered.
        
        buffer.position(position);
        return data;
    }

    /**
     * Converts the specified string value into a byte buffer.  This method
     * is a convenience method heavily used by this gateway, since Cassandra
     * stores its key values as binary values.
     *
     * @param value the string value to convert into a byte buffer.
     *
     * @return a byte buffer representation of the specified string.
     * 
     * @throws Exception can occur during the byte buffer conversion to string.
     */
    @Deprecated
	public ByteBuffer getByteBuffer(String value) throws Exception {
		return encoder.encode(CharBuffer.wrap(value));
	}

    /**
     * Gets the keyspace this gateway is associated with.
     * 
     * @return the keyspace this gateway is associated with.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Gets the Cassandra node this gateway point to.
     *
     * @return the Cassandra node this gateway point to.
     */
    public CassandraNode getNode() {
        return node;
    }
}