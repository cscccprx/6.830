package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final Field NO_GROUPING = new IntField(-1);
    private int gbField;
    private int aField;
    private Type gbFieldType;
    private Op op;
    private Gbhandler gbhandler;
    private HashMap<Field, Integer> gbMap = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException();
        }
        this.aField = afield;
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.op = what;
        this.gbhandler = getHandler(this.op);
    }

    public Gbhandler getHandler(Op op) {
        switch (this.op) {
            case COUNT:return new CountHandler();
        }
        return null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = this.gbFieldType == null ? NO_GROUPING : tup.fieldList.get(this.gbField);
        gbhandler.handler(key,((StringField)tup.fieldList.get(this.aField)).getValue());
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Type[] types = new Type[]{this.gbFieldType, Type.INT_TYPE};
        Type[] typesNoGroup = new Type[]{Type.INT_TYPE};
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td = null;
        for (Map.Entry<Field, Integer> fieldIntegerEntry : gbMap.entrySet()) {
            Field key = fieldIntegerEntry.getKey();
            Field value = new IntField(fieldIntegerEntry.getValue());
            if (this.gbFieldType == null) {
                // no_grouping
                td = new TupleDesc(typesNoGroup);
                Tuple tuple = new Tuple(td);
                tuple.setField(0,value);
                tuples.add(tuple);
            }else {
                td = new TupleDesc(types);
                Tuple tuple = new Tuple(td);
                tuple.setField(0,key);
                tuple.setField(1,value);
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    abstract class Gbhandler {
        Gbhandler(){}
        abstract void handler(Field key, String value);
    }

    private class CountHandler extends Gbhandler{
        @Override
        void handler(Field key, String value) {
            if (gbMap.containsKey(key)) {
                gbMap.put(key, gbMap.get(key) + 1);
            }else {
                gbMap.put(key, 1);
            }
        }
    }

}
