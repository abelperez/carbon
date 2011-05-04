package com.mindplex.cassandra;

import java.util.List;

/**
 *
 * @author Abel Perez
 */
public interface CassandraGateway
{
    /**
     *
     * @param columnFamily
     * @param rowid
     * @param column
     * @throws Exception
     */
    void delete(String columnFamily, String rowid, String column) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @param pairs
     * @throws Exception
     */
    void deleteAll(String columnFamily, String rowid, Pair[] pairs) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @param key
     * @return
     * @throws Exception
     */
    String findColumn(String columnFamily, String rowid, String key) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @return
     * @throws Exception
     */
    List<Pair> findColumnsSliceRange(String columnFamily, String rowid) throws Exception;

    /**
     *
     * @param columnFamily
     * @param keys
     * @return
     * @throws Exception
     */
    List<Pair> findByKeyRange(String columnFamily, List<String> keys) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @param keys
     * @return
     * @throws Exception
     */
    List<Pair> findColumns(String columnFamily, String rowid, List<String> keys) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @param pair
     * @throws Exception
     */
    void insert(String columnFamily, String rowid, Pair pair) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @param pairs
     * @throws Exception
     */
    void insert(String columnFamily, String rowid, Pair[] pairs) throws Exception;

    /**
     *
     * @param columnFamily
     * @param rowid
     * @param pairs
     * @throws Exception
     */
    void insertAll(String columnFamily, String rowid, Pair[] pairs) throws Exception;

    /**
     *
     * @param columnFamily
     * @param superColumnName
     * @param rowid
     * @param pairs
     * @throws Exception
     */
    void insertAllSuperColumns(String columnFamily, String superColumnName, String rowid, Pair[] pairs) throws Exception;

    /**
     * 
     * @throws Exception
     */
    void discover() throws Exception;

    /**
     * 
     * @throws Exception
     */
    void deleteBySliceRange() throws Exception;
}
