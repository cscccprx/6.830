package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    // TransactionId t, OpIterator child, int tableId
    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private List<Tuple> tuple = new ArrayList<>();
    private Iterator<Tuple> it ;
    private boolean isOpen = true;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        while (this.child.hasNext()) {
            Tuple next = this.child.next();
            try {
                bufferPool.insertTuple(this.transactionId,this.tableId, next);
                count++;
            }catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        Tuple tuple1 = new Tuple(this.tupleDesc);
        tuple1.setField(0,new IntField(count));
        this.tuple.add(tuple1);
        it = this.tuple.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        it = null;
        super.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = this.tuple.iterator();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext() && isOpen) {
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
