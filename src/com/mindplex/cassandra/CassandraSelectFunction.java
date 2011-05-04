package com.mindplex.cassandra;

/**
 *
 */
public interface CassandraSelectFunction<T, R> 
{
    /**
     * 
     * @param client
     * @return
     * @throws Exception
     */
    public R execute(T client) throws Exception;
}
