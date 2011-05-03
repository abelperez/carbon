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
