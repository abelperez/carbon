package com.mindplex.cassandra;

/**
 *
 * @author Abel Perez
 */
public interface CassandraFunction<T>
{
    /**
     * 
     * @param client
     * @throws Exception
     */
    public void execute(T client) throws Exception;
}
