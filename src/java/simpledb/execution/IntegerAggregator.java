package simpledb.execution;

import javafx.util.Pair;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    public final Field NO_GROUPING = new IntField(-1);
    private int gbField;
    private int aField;
    private Type gbFieldType;
    private Op op;
    private Map<Field, Integer> gbMap = new HashMap<>();
    private GbHandler gbHandler;
    private Iterator<Tuple> it = null;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.gbHandler = getHander(this.op);
    }

    public GbHandler getHander(Op op) {
        switch (this.op){
            case MIN: return new MinHandler();
            case AVG: return new AvgHandler();
            case SUM: return new SumHandler();
            case COUNT: return new CountHandler();
            case MAX: return new MaxHandler();
            case SC_AVG:
            case SUM_COUNT:
            default:return null;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbField != ((IntField)NO_GROUPING).getValue()
                && !tup.tupleDesc.getTdItemList().get(this.gbField).fieldType.equals(this.gbFieldType)) {
            throw new IllegalArgumentException();
        }
        Field key = this.NO_GROUPING.equals(new IntField(this.gbField)) ? NO_GROUPING :
                tup.fieldList.get(this.gbField);
        gbHandler.handler(key,((IntField)tup.fieldList.get(this.aField)).getValue());
    }

    private abstract class GbHandler {
        public GbHandler(){}
        abstract void handler(Field key, Integer value);
    }

    private class MinHandler extends GbHandler {
        @Override
        void handler(Field key, Integer value) {
            if (gbMap.containsKey(key)) {
                gbMap.put(key,Math.min(gbMap.get(key), value));
            }else {
                gbMap.put(key, value);
            }
        }
    }

    private class MaxHandler extends GbHandler {
        @Override
        void handler(Field key, Integer value) {
            if (gbMap.containsKey(key)) {
                gbMap.put(key,Math.max(gbMap.get(key), value));
            }else {
                gbMap.put(key, value);
            }
        }
    }

    private class SumHandler extends GbHandler {
        @Override
        void handler(Field key, Integer value) {
            if (gbMap.containsKey(key)) {
                gbMap.put(key,gbMap.get(key) + value);
            }else {
                gbMap.put(key, value);
            }
        }
    }

    private class CountHandler extends GbHandler {

        @Override
        void handler(Field key, Integer value) {
            if (gbMap.containsKey(key)) {
                gbMap.put(key,gbMap.get(key) + 1);
            }else {
                gbMap.put(key, 1);
            }
        }
    }

    private class AvgHandler extends GbHandler {
        Map<Field, Integer> countMap = new HashMap<>();
        Map<Field, Integer> avgMap = new HashMap<>();
        @Override
        void handler(Field key, Integer value) {
            if (countMap.containsKey(key) && avgMap.containsKey(key)) {
                countMap.put(key,countMap.get(key) + 1);
                avgMap.put(key, avgMap.get(key) + value);
            }else {
                avgMap.put(key, value);
                countMap.put(key, 1);
            }
            gbMap.put(key,avgMap.get(key) / countMap.get(key));
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tupleList = new ArrayList<>();
        Type[] types = new Type[]{this.gbFieldType,Type.INT_TYPE};
        Type[] typesNoGroup = new Type[]{Type.INT_TYPE};
        TupleDesc td = null;
        for (Map.Entry<Field, Integer> fieldIntegerEntry : gbMap.entrySet()) {
            Field key = fieldIntegerEntry.getKey();
            Field value = new IntField(fieldIntegerEntry.getValue());
            if (this.gbField == ((IntField)NO_GROUPING).getValue()) {
                td = new TupleDesc(typesNoGroup);
                Tuple tuple = new Tuple(td);
                tuple.setField(0,value);
                tupleList.add(tuple);
            }else {
                td = new TupleDesc(types);
                Tuple tuple = new Tuple(td);
                tuple.setField(0,key);
                tuple.setField(1,value);
                tupleList.add(tuple);
            }
        }
        return new TupleIterator(td,tupleList);
    }

}
