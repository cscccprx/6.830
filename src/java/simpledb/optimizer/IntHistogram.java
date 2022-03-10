package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private long minValue;
    private long maxValue;
    private int nTup;
    double capacity;
    private List<InnerBucket> innerBucketList = new ArrayList<>();

    /**
     * all element [left,right]
     */
    class InnerBucket {
        // 这里其实并不需要真正的找个数据结构给存起来，因为只是需要统计数量
        // 不过这样写可以  只是内存会变大
//        Queue<Field> q =  new PriorityQueue<>(new Comparator<Field>() {
//            @Override
//            public int compare(Field o1, Field o2) {
//                return ((IntField)o1).getValue() - ((IntField)o2).getValue();
//            }
//        });
        int count = 0;
        long left;
        long right;
        int index;

        InnerBucket(int left, int right, int index) {
            this.left = (long)left;
            this.right = (long)right;
            this.index = index;
        }

        public void setField(Field field){
            count++;
        }
        public int getWidth(){
            return (int)(this.right - this.left + 1);
        }
        public int getHeight(){
            return count;
        }

    }
    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.minValue = (long)min;
        this.maxValue = (long)max;
        this.capacity = 1.0 * (this.maxValue - this.minValue + 1) / buckets;
        int index = 0;
        while (index < buckets) {
            int left = (int)Math.ceil(min + index * capacity);
            int right = (int)Math.ceil(min + (index + 1) * capacity) - 1;
            if (left > right){
                right = left;
            }
            innerBucketList.add(new InnerBucket(left, right, index++));
        }
        this.nTup = this.getTupleNums();
    }

    private int getTupleNums() {
        int count = 0;
        for (InnerBucket innerBucket : innerBucketList) {
            count = count + innerBucket.count;
        }
        return count;
    }

    private int getIndex(int v) {
        long a = (long)v;
        return (int)((a - this.minValue)/this.capacity);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < minValue | v > maxValue) {
            return;
        }
        innerBucketList.get(getIndex(v)).setField(new IntField(v));
        this.nTup++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        switch (op) {
            case EQUALS:{
                if (v < minValue)
                    return 0.0;
                if (v > maxValue)
                    return 0.0;
                int index = getIndex(v);
                InnerBucket innerBucket = innerBucketList.get(index);
                // 等价于 (1.0 * innerBucket.getHeight() / innerBucket.getWidth()) / nTup
                return ((double)innerBucket.getHeight() / innerBucket.getWidth()) / nTup;
            }
            case GREATER_THAN:{
                if (v <= minValue)
                    return 1.0;
                if (v >= maxValue)
                    return 0.0;
                int index = getIndex(v);
                double sum = 0.0;
                InnerBucket innerBucket1 = innerBucketList.get(index);
                sum = 1.0 * innerBucket1.count * (innerBucket1.right - v) / innerBucket1.getWidth();
                for (int i = index + 1; i < buckets; i++) {
                    sum += innerBucketList.get(i).count;
                }
                return sum / nTup;
            }
            case LESS_THAN:
                if (v <= minValue)
                    return 0.0;
                if (v >= maxValue)
                    return 1.0;
                int index = getIndex(v);
                double sum = 0.0;
                InnerBucket innerBucket1 = innerBucketList.get(index);
                sum = 1.0 * innerBucket1.count * (v - innerBucket1.left) / innerBucket1.getWidth();
                for (int i = index - 1; i >= 0; i--) {
                    sum += innerBucketList.get(i).count;
                }
                return sum / nTup;
            case GREATER_THAN_OR_EQ:return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case LESS_THAN_OR_EQ:return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN,v);
            case NOT_EQUALS:return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:new UnsupportedOperationException();
        }
        return -1.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0 * this.nTup / buckets;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
