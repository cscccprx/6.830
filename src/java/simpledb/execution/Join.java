package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.RecordId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;
import java.util.stream.Stream;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate joinPredicate;
    private OpIterator opIterator1;
    private OpIterator opIterator2;
    private TupleDesc joinTupleDesc;
    private Iterator<Tuple> it;
    private List<Tuple> tuples = new ArrayList<>();


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.opIterator1 = child1;
        this.opIterator2 = child2;
        List<TupleDesc.TDItem> tupleDescList = new ArrayList<>();
        for (TupleDesc.TDItem tdItem : opIterator1.getTupleDesc().tdItemList) {
            tupleDescList.add(tdItem);
        }
        for (TupleDesc.TDItem tdItem : opIterator2.getTupleDesc().tdItemList) {
            tupleDescList.add(tdItem);
        }
        this.joinTupleDesc = new TupleDesc(tupleDescList);
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return joinTupleDesc.tdItemList.get(joinPredicate.getField1()).fieldName;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return joinTupleDesc.tdItemList.get(joinPredicate.getField2()).fieldName;
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.joinTupleDesc;
    }

    // merge sort join 和 hash join 都是用在 等价查询 也就是 Equals prediction 中
    // hash join 就是先扫描小表，生成hash ，值相同的放在一个桶里，然后在扫描大表，根据hash 值去到桶里寻找
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.opIterator1.open();
        this.opIterator2.open();
        while (this.opIterator1.hasNext()) {
            Tuple next1 = opIterator1.next();
            Tuple tuple = null;
            while (this.opIterator2.hasNext()) {
                Tuple next2 = opIterator2.next();
                if (joinPredicate.filter(next1,next2)) {
                    // function addAll()  if parent list is [null,null],
                    // addAll() will [null,null,1,2]   special case
                    tuple = new Tuple(this.joinTupleDesc);
                    tuple.fieldList = new ArrayList<>(next1.fieldList);
                    tuple.fieldList.addAll(next2.fieldList);
                    tuples.add(tuple);
                }
            }
            this.opIterator2.rewind();
        }
        it = tuples.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        it = null;
        this.opIterator1.close();
        this.opIterator2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = tuples.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.opIterator1, this.opIterator2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.opIterator1 = children[0];
        this.opIterator2 = children[1];
    }

}