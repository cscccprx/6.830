package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    public TupleDesc tupleDesc;
    public List<Field> fieldList = new ArrayList<>();
    public RecordId recordId;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
        for (int i = 0; i < tupleDesc.getTdItemList().size(); i++) {
            fieldList.add(null);
        }

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= tupleDesc.numFields()) {
            throw new IllegalArgumentException();
        }
        fieldList.set(i, f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        return fieldList.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
//        throw new UnsupportedOperationException("Implement this");
        return fieldList.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return fieldList.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        tupleDesc.clear();
        tupleDesc.setTdItemList(td.getTdItemList());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null){
            return false;
        }
        Tuple t2 = (Tuple) o;
        Tuple t1 = this;
        if (this.getTupleDesc().numFields() != t2.getTupleDesc().numFields())
            return false;

        for (int i = 0; i < t1.getTupleDesc().numFields(); ++i) {
            if (!(t1.getTupleDesc().getFieldType(i).equals(t2.getTupleDesc().getFieldType(i))))
                return false;
            if (!(t1.getField(i).equals(t2.getField(i))))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tupleDesc, fieldList, recordId);
    }
}
