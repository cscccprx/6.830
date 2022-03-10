package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;
    private final HeapFile tableHeapFile;
    private final TupleDesc tupleDesc;
    private int count = 0;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;
    private Map<Integer, IntHistogram> intHistogramMap = new HashMap<>();
    private Map<Integer, StringHistogram> stringHistogramMap = new HashMap<>();

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        this.tableHeapFile = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
        SeqScan seqScan = new SeqScan(new TransactionId(), tableid);
        this.tupleDesc = seqScan.getTupleDesc();
        int numFields = tupleDesc.numFields();
        int[] mins = new int[numFields];
        int[] maxs = new int[numFields];
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            // init types mins maxs
            mins[i] = Integer.MAX_VALUE;
            maxs[i] = Integer.MIN_VALUE;
        }
        try {
            seqScan.open();
            // 这里为什么要选出 max 和 min，直接用Integer.max or min 不是也可以吗？
            // 因为这样的话 每个桶的大小就太大了，也就是 width 太宽了 >> 单个桶的tuple 数，
            // 一做除法 值就接近于0了， 对于精度要求不高的来讲 可以
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                for (int i = 0; i < numFields; i++) {
//                    int val = tuple.getField(i).hashCode();
                    if (tuple.getField(i).getType() == Type.INT_TYPE) {
                        int val = ((IntField)tuple.getField(i)).getValue();
                        mins[i] = val<mins[i]?val:mins[i];
                        maxs[i] = val>maxs[i]?val:maxs[i];
                    }
                }
            }
            seqScan.rewind();
            while (seqScan.hasNext()) {
                Tuple next = seqScan.next();
                this.count++;
                for (int i = 0; i <  tupleDesc.getTdItemList().size(); i++) {
                    Field field = next.getField(i);
                    if (field.getType() == Type.INT_TYPE) {
                        if (intHistogramMap.containsKey(i)) {
                            intHistogramMap.get(i).addValue(((IntField)field).getValue());
                        }else {
                            IntHistogram value = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
                            value.addValue(((IntField)field).getValue());
                            intHistogramMap.put(i, value);
                        }
                    }else {
                        if (stringHistogramMap.containsKey(i)) {
                            stringHistogramMap.get(i).addValue(((StringField)field).getValue());
                        }else {
                            StringHistogram value = new StringHistogram(NUM_HIST_BINS);
                            value.addValue(((StringField)field).getValue());
                            stringHistogramMap.put(i, value);
                        }
                    }
                }
            }
            seqScan.close();
        } catch (DbException | TransactionAbortedException exception) {
            exception.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return 1.0 * tableHeapFile.numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.count * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return tupleDesc.getTdItemList().get(field).fieldType == Type.INT_TYPE ?
                intHistogramMap.get(field).avgSelectivity() : stringHistogramMap.get(field).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        return tupleDesc.getTdItemList().get(field).fieldType == Type.INT_TYPE ?
                intHistogramMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue())
                : stringHistogramMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return count;
    }

}
