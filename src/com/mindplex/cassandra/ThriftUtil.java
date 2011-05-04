package com.mindplex.cassandra;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.*;

/**
 *
 * @author Abel Perez
 */
public class ThriftUtil
{
    /** */
    public static final String SYSTEM_KEYSPACE = "system";

    /** */    
    private static final String ALL = "";

    /**
     *
     * @param key
     * @param val
     * @return
     */
    public static Column getColumn(String key, String val) {
        return new Column()
                .setName(key.getBytes())
                .setValue(val.getBytes())
                .setTimestamp(System.currentTimeMillis());
    }

    /**
     *
     * @param pair
     * @return
     */
    public static Column getColumn(Pair pair) {
        return new Column()
                .setName(pair.getKey().getBytes())
                .setValue(pair.getVal().getBytes())
                .setTimestamp(System.currentTimeMillis());
    }

    /**
     *
     * @param column
     * @return
     */
    public static Mutation getMutation(Column column) {

        ColumnOrSuperColumn csc = new ColumnOrSuperColumn();
        csc.setColumn(column);
        csc.setColumnIsSet(true);

        Mutation mutation = new Mutation()
                .setColumn_or_supercolumn(csc);
        mutation.setColumn_or_supercolumnIsSet(true);
        return mutation;
    }

    /**
     *
     * @param column
     * @return
     */
    public static Mutation getMutation(SuperColumn column) {

        ColumnOrSuperColumn csc = new ColumnOrSuperColumn();
        csc.setSuper_column(column);
        csc.setSuper_columnIsSet(true);

        Mutation mutation = new Mutation()
                .setColumn_or_supercolumn(csc);
        mutation.setColumn_or_supercolumnIsSet(true);
        return mutation;
    }

    /**
     *
     * @param columnFamily
     * @param column
     * @return
     * @throws Exception
     */
    public static ColumnPath getColumnPath(String columnFamily, String column) throws Exception {
        ColumnPath path = new ColumnPath();
        path.column_family = columnFamily;
        path.column = toByteBuffer(column);
        return path;
    }

    /**
     *
     * @return
     */
    public static SlicePredicate all() {
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setStart(ALL.getBytes());
        range.setFinish(ALL.getBytes());
        predicate.setSlice_range(range);
        return predicate;
    }

    /**
     *
     * @return
     */
    public static KeyRange allKeyRange() {
        KeyRange keyrange = new KeyRange();
        keyrange.start_key = toByteBuffer(ALL);
        keyrange.end_key = toByteBuffer(ALL);
        return keyrange;
    }

    /**
     * 
     * @param value
     * @return
     */
    public static ByteBuffer toByteBuffer(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }   
}