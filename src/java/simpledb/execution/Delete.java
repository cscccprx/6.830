package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator opIterator;
    private TupleDesc tupleDesc;
    private List<Tuple> tuples = new ArrayList<>();
    private Iterator<Tuple> it;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.transactionId = t;
        this.opIterator = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.opIterator.open();
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        while (this.opIterator.hasNext()) {
            try {
                bufferPool.deleteTuple(this.transactionId, this.opIterator.next());
                count++;
            }catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0, new IntField(count));
        tuples.add(tuple);
        it = tuples.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        this.opIterator.close();
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = tuples.iterator();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
        return new OpIterator[]{this.opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.opIterator = children[0];
    }

}
