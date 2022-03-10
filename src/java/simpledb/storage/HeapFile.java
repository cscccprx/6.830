package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * 注 每个表有一个HeapPage 对象，也就是说明只有一个TupleDesc
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    public File f ;
    public TupleDesc tupleDesc;
    public Map<PageId,HeapPage> heapPageMap = new HashMap<>();


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile r = new RandomAccessFile(f, "r");
            byte[] buf = new byte[BufferPool.getPageSize()];
            int len = 0;
            r.seek(pid.getPageNumber() * BufferPool.getPageSize());
            if (r.read(buf) == -1) {
                return null;
            }
            r.close();
            return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()),buf);
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile wr = new RandomAccessFile(this.f, "rw");
        wr.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        wr.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 参考别人实现 就是不知道本地方法求f.length()的开销 应该会比自己实现的小
        return (int)this.f.length() / BufferPool.getPageSize();

        // 以下为自己实现，考虑到每次计算num都需要与disk 交互，成本太高，所以设置缓存
        // 但是一旦文件改变，就会导致计算偏差，这里比较矛盾
//        if (this.pageNum != -1){
//            return this.pageNum;
//        }
//        int count = 0;
//        try {
//            RandomAccessFile r = new RandomAccessFile(f,"r");
//            byte[] buf = new byte[BufferPool.getPageSize()];
//            while ((r.read(buf)) != -1) {
//                count++;
//                r.seek(count * BufferPool.getPageSize());
//            }
//            r.close();
//            this.pageNum = count;
//            return this.pageNum;
//        }catch (IOException e){
//
//            e.printStackTrace();
//        }
//        return -1;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifyPages = new ArrayList<>();
        boolean isSuccessInsert = false;
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i),Permissions.READ_WRITE);
            try {
                page.insertTuple(t);
                isSuccessInsert = true;
                modifyPages.add(page);
                break;
            }catch (DbException dbException) {
                continue;
            }
        }
        // page created lastly, so there is empty
        if (!isSuccessInsert) {
            HeapPageId id = new HeapPageId(this.getId(), numPages());
            // 先拿到写锁  防止另一个事务都写同一个页 导致之前的内容覆盖
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,id,Permissions.READ_WRITE);
            byte[] emptyPageData = HeapPage.createEmptyPageData();
            RandomAccessFile w = new RandomAccessFile(this.f, "rw");
            w.seek(f.length());
            w.write(emptyPageData);
            w.close();
            // notice!!  all scope use Database.getBufferPool (a single instance)
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid,id,Permissions.READ_WRITE);
            heapPage.insertTuple(t);
            modifyPages.add(heapPage);
        }
        return modifyPages;
        // not necessary for lab1
    }



    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifyPages = new ArrayList<>();
        HeapPageId pageId = (HeapPageId)t.getRecordId().getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId,Permissions.READ_WRITE);
        page.deleteTuple(t);
        modifyPages.add(page);
        return modifyPages;
        // not necessary for lab1
    }



    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator(){
            private int pid = 0;
            private HeapPage curPage;
            private int totalPageNum = numPages();
            Iterator<Tuple> iterator;
            private boolean open = false;

            public HeapPage getPage() throws TransactionAbortedException, DbException {
                curPage = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(),pid++),Permissions.READ_ONLY);
                return curPage;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.open = true;
                HeapPage page = getPage();
                this.iterator = page.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!this.open) {
                    return false;
                }
                // prevent last page but still exist tuple  this option pid != totalPageNum
                // 一旦两个空page 连着 用if 就会有问题，改成while
                while (!this.iterator.hasNext() && pid != totalPageNum) {
                    HeapPage page = getPage();
                    this.iterator = page.iterator();
                }
                return this.iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!this.open) {
                    throw new NoSuchElementException();
                }
                return this.iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!this.open) {
                    throw new NoSuchElementException();
                }
                pid = 0;
                HeapPage nowCurPage = (HeapPage) getPage();
                this.iterator = nowCurPage.iterator();
            }

            @Override
            public void close() {
                this.open = false;
                pid = 0;
            }
        };
    }

}

