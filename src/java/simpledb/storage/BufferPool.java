package simpledb.storage;


import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
// 加锁粒度是page  性能更好的话就加行锁， 行锁阻止脏读和不可重复读，  加间隙锁防止幻读，也就是插入记录的前面不允许新的插入
    // 性能最好的就是 steal 和 no force
public class BufferPool {
    private static Logger logger ;
    static {
        logger = Logger.global.getLogger(Logger.GLOBAL_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        FileHandler fileHandler = null;
        try {
            fileHandler = new FileHandler("8888g.log");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        fileHandler.setLevel(Level.INFO);
        logger.addHandler(fileHandler);
//        logger.info("aaa");
    }

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final long LOCK_TIME_OUT = 2000;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private int numPages;

    private Map<PageId,Page> pageMap = new ConcurrentHashMap<>();

    private LockManager lockManager;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        logger.log(Level.INFO, "init buffer pool");
        this.numPages = numPages;
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
// 这里加锁 但没释放  释放的时机应该是 当这个 transaction 释放的时候
// TODO 现在还有几个问题
//  1。 要把为什么在这里 lock.unlock 加上就会有问题  留在之后解答吧 先准备面试
//  2。 为什么获取了写锁了 还能成功获取读锁？？？？  fix：为了防止脏读
//  3。 看一下测试用例里面的注释 看是不是符合预期
//  4。 看 是否和提交事务有关？
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        // some code goes here
        LockType lockType = perm == Permissions.READ_ONLY ? LockType.SHARE : LockType.EXCLUDE;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while (!lockManager.acquireLock( pid,tid, lockType)) {
            logger.log(Level.INFO,"事务 ：" + tid.getId() + " page id : " + pid.getPageNumber() + " 未获取到" +
                    lockType.name()+"锁");
            long end = System.currentTimeMillis();
            if (end - start > timeout) {
                throw new TransactionAbortedException();
            }
        }
        logger.log(Level.INFO,"事务id : " + tid.getId() + " page id : " + (pid).getPageNumber() +
                "获取" + lockType.name() +"锁");
        if (pageMap.containsKey(pid)){
            return pageMap.get(pid);
        }
        if (pageMap.size() + 1 > this.numPages) {
            // evict one page using fifo policy
            try {
                evictPage();
            }catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        Catalog.Table table = Catalog.getTable(pid.getTableId());
//        HeapFile heapFile = (HeapFile) table.file;
        DbFile file = table.file;
        Page page = file.readPage(pid);
        if (file == null || page == null) {
            return null;
        }
        pageMap.put(pid, page);
        return pageMap.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.unsafeReleasePage(tid,pid);

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid)  {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    // 这个方法调用的时候 一般已经获取到锁了  所以对于flush dirty 就不需要额外加锁
    // force策略  一旦commit 就立即刷盘
    // steal/no-steal:
    //      是否允许一个uncommitted的事务将修改更新到磁盘，如果是steal策略，
    //      那么此时磁盘上就可能包含uncommitted的数据，因此系统需要记录undo log，
    //      以防事务abort时进行回滚（roll-back）。如果是no steal策略，
    //      就表示磁盘上不会存在uncommitted数据，因此无需回滚操作，也就无需记录undo log。
    //
    // force/no-force:
    //      force策略表示事务在committed之后必须将所有更新立刻持久化到磁盘，
    //      这样会导致磁盘发生很多小的写操作（更可能是随机写）。
    //      no-force表示事务在committed之后可以不立即持久化到磁盘， 这样可以缓存很多的更新批量持久化到磁盘，
    //      这样可以降低磁盘操作次数（提升顺序写），但是如果committed之后发生crash，
    //      那么此时已经committed的事务数据将会丢失（因为还没有持久化到磁盘），因此系统需要记录redo log，在系统重启时候进行前滚（roll-forward）操作。
    // steal/no-steal主要决定了磁盘上是否会包含uncommitted的数据。force/no-force主要决定了磁盘上是否会不包含已经committed的数据。
    // https://www.devbean.net/2016/05/how-database-works-8/#:~:text=STEAL%20%E5%92%8C%20FORCE%20%E7%AD%96%E7%95%A5%20%E7%94%B1%E4%BA%8E%E6%80%A7%E8%83%BD%E5%8E%9F%E5%9B%A0%EF%BC%8C%20%E7%AC%AC%205%20%E6%AD%A5%E5%8F%AF%E8%83%BD%E4%BC%9A%E5%9C%A8%E4%BA%8B%E5%8A%A1%E6%8F%90%E4%BA%A4%E4%B9%8B%E5%90%8E%E5%AE%8C%E6%88%90,NO-FORCE%20%E7%AD%96%E7%95%A5%20%E3%80%82%20%E6%95%B0%E6%8D%AE%E5%BA%93%E4%B9%9F%E5%8F%AF%E4%BB%A5%E9%80%89%E6%8B%A9%20FORCE%20%E7%AD%96%E7%95%A5%EF%BC%88%E4%B9%9F%E5%B0%B1%E6%98%AF%E8%AF%B4%EF%BC%8C%E7%AC%AC%205%20%E6%AD%A5%E5%BF%85%E9%A1%BB%E5%9C%A8%E6%8F%90%E4%BA%A4%E4%B9%8B%E5%89%8D%E5%AE%8C%E6%88%90%EF%BC%89%E9%99%8D%E4%BD%8E%E6%81%A2%E5%A4%8D%E6%9C%9F%E9%97%B4%E7%9A%84%E5%B7%A5%E4%BD%9C%E8%B4%9F%E8%BD%BD%E3%80%82
    public void transactionComplete(TransactionId tid, boolean commit)  {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            List<Page> pages = findPagesListByTid(tid);
            if (commit) {
//                Database.getLogFile().logCommit(tid);
                flushPages(tid,pages);
            }else {
                // abort reload this page from disk
                revertTransaction(pages);
            }
            for (PageId pageId : pageMap.keySet()) {
                if (lockManager.holdsLock(tid,pageId)) {
                    unsafeReleasePage(tid, pageId);
                }
            }
        }catch (DbException | IOException d) {
            d.printStackTrace();
        }

    }

    private void revertTransaction(List<Page> pages) throws DbException {
        // discard dirty page and then reload these pages
        if (pages == null || pages.size() == 0) {
            return;
        }
        for (Page page : pages) {
            if (page.isDirty()!=null) {
                discardPage(page.getId());
                Page page1 = Database.getCatalog().getDatabaseFile(page.getId().getTableId())
                        .readPage(page.getId());
                page1.markDirty(false, null);
                pageMap.put(page.getId(), page1);
            }
        }
    }

    private List<Page> findPagesListByTid(TransactionId tid) {
        ArrayList<Page> pages = new ArrayList<>();
        if (tid == null) {
            return pages;
        }
        for (PageId pageId : pageMap.keySet()) {
            Page page = pageMap.get(pageId);
            //  page.isDirty() == null || tid.equals(page.isDirty())
            if (page != null && (tid.equals(page.isDirty()) )) {
                // find the transaction to the dirty page to flush
                pages.add(page);
            }
        }
        return pages;
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Catalog.Table table = Catalog.getTable(tableId);
        DbFile heapFile = table.file;
        List<Page> modifyPages = heapFile.insertTuple(tid, t);
        updateBufferPool(modifyPages, tid);
    }

    private void updateBufferPool(List<Page> modifyPages, TransactionId tid) {
        // 这里不需要在evict page 了，因为上层调用insert 和 delete 的时候 如果没有错误的话
        // 现在的页面都在buffer pool中
        // 特殊case ： 一旦insert 插入了 第一个page 但是 这个page 被置换了
        for (Page modifyPage : modifyPages) {
            // update in fact
            // TODO 这里对于 modifyPage 的写操作 加不加锁待考虑，因为是写操作，会不会性能不是很好？
            modifyPage.markDirty(true, tid);
            this.pageMap.put(modifyPage.getId(),  modifyPage);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Catalog.Table table = Catalog.getTable(t.recordId.getPageId().getTableId());
        DbFile heapFile = table.file;
        List<Page> pages = heapFile.deleteTuple(tid, t);
        updateBufferPool(pages, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId : pageMap.keySet()) {
            try {
                if (pageMap.get(pageId).isDirty() != null) {
                    // dirty page
                    flushPage1(pageId);
                }
            } catch (DbException dbException) {
                dbException.printStackTrace();
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!pageMap.containsKey(pid)) {
            return;
//            throw new DbException("discard exception");
        }
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException, DbException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Catalog.getTable(pid.getTableId()).file;
        if (!pageMap.containsKey(pid)) {
            throw new DbException("buffer pool write page exception");
        }
        Page page = pageMap.get(pid);
        Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(),page);
        Database.getLogFile().force();
        file.writePage(page);
        page.markDirty(false, null);

    }
    // 这个方法在写盘的时候 如果没有commit 那么这个页一直都是dirty 的
    private synchronized  void flushPage1(PageId pid) throws IOException, DbException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Catalog.getTable(pid.getTableId()).file;
        if (!pageMap.containsKey(pid)) {
            throw new DbException("buffer pool write page exception");
        }
        Page page = pageMap.get(pid);
        Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(),page);
        Database.getLogFile().force();
        file.writePage(page);

    }

    /** Write all pages of the specified transaction to disk.
     */
// maybe a simple optimization ?  batch flush disk
    public synchronized  void flushPages(TransactionId tid, List<Page> pages) throws IOException, DbException {
        // some code goes here
        // not necessary for lab1|lab2

        for (Page page : pages) {
            page.setBeforeImage();
            if ((page).isDirty() != null) {
                flushPage(page.getId());
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
// 我觉得这里的 synchronized 是没有必要的 如果只在 getPage()中调用的话
    private synchronized  void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
        // no steal 表示 事务提交了之后 才会把dirty page flush 到磁盘。所以置换的时候 不能置换脏页
        // 如果已经 evict 的页还持有锁，需要先把这个锁释放掉  否则会有死锁
        boolean isEvict = false;
        for (PageId id : pageMap.keySet()) {
            if (pageMap.get(id).isDirty() == null) {
                // no dirty page flush
                discardPage(id);
                logger.log(Level.INFO, "evict page id: " + id.getPageNumber());
                lockManager.removeLock(id);
                isEvict = true;
                break;
            }
        }

        if (!isEvict) {
            throw new DbException("no page evict");
        }

    }


    enum LockType{
        SHARE,EXCLUDE
    }

    class LockReadWrite{
        LockType lockType;
        TransactionId tid;

        LockReadWrite(LockType lockType, TransactionId tid) {
            this.lockType = lockType;
            this.tid = tid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockReadWrite that = (LockReadWrite) o;
            return lockType == that.lockType &&
                    Objects.equals(tid, that.tid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lockType, tid);
        }
    }

    /**
     * page-level read-write lock
     */
    class LockManager {

        ConcurrentHashMap<PageId, CopyOnWriteArrayList<LockReadWrite>> lockMapList = new ConcurrentHashMap<>();
//        HashMap<PageId, List<LockReadWrite>> lockMapList = new HashMap<>();

        LockManager() {

        }


        // 这里还是要加上synchronized  那么这里有同学就会产生疑问，不是读写锁么，如果在这里加了互斥机制的话
        // 即使是读读操作也会竞争锁了。 但是要考虑这样一个问题，如果两个线程同时进入到这个方法，注意是同时，不同事务一个
        // 想要申请读锁，一个想要申请写锁，那么这两个锁都会申请成功，因为在判断 ！lockMapList.containsKey(pageId)
        // 这个条件的时候 都是会成功的，但是这样是不符合预期的，所以要使用synchronized。
        // 但是按照我的代码实现，在lockManager类中所用到的 map 和 list 都是线程安全的，所以可以不加synchronized
        boolean acquireLock(PageId pageId , TransactionId tid, LockType lockType) {
            if (!lockMapList.containsKey(pageId)) {
                CopyOnWriteArrayList<LockReadWrite> list = new CopyOnWriteArrayList<>();
                LockReadWrite lockReadWrite = new LockReadWrite(lockType, tid);
                list.add(lockReadWrite);
                lockMapList.put(pageId, list);
                return true;
            }
            // 包含，要根据事务来获得锁
            boolean isHaveWriteLock = false;
            for (LockReadWrite lockReadWrite : lockMapList.get(pageId)) {
                // 判断该页是否有写锁
                if (lockReadWrite.lockType == LockType.EXCLUDE) {
                    isHaveWriteLock = true;
                }

                if (lockReadWrite.tid.equals(tid)) {
                    if (lockReadWrite.lockType == lockType){
                        // 同一个事务获取写/读锁是可以的 锁的重入
                        return true;
                    }else {
                        if (lockReadWrite.lockType == LockType.EXCLUDE) {
                            // 该事务已有写锁  申请读锁
                            return true;
                        }else {
                            // 该事务已有读锁  申请写锁
                            if (lockMapList.get(pageId).size() == 1) {
                                // 如果该页只有一个事务有这个锁，升级为写锁
                                lockReadWrite.lockType = LockType.EXCLUDE;
                                return true;
                            }else {
                                return false;
                            }
                        }
                    }
                }
            }
            // 该页如果有一个写锁，其他事务不能获得锁
            if (!isHaveWriteLock) {
                if (lockType == LockType.EXCLUDE) {
                    return false;
                }
                LockReadWrite lockReadWrite = new LockReadWrite(lockType, tid);
                lockMapList.get(pageId).add(lockReadWrite);
                return true;
            }
            return false;
        }

        public boolean holdsLock(TransactionId tid, PageId p) {
            if (!lockMapList.containsKey(p)) {
                return false;
            }
            for (LockReadWrite lockReadWrite : lockMapList.get(p)) {
                if (lockReadWrite.tid.equals(tid)){
                    return true;
                }
            }
            return false;
        }

        public void removeLock(PageId pageId) {

            lockMapList.remove(pageId, lockMapList.get(pageId));
        }

        public void unsafeReleasePage(TransactionId tid, PageId pid) {
            if (!lockMapList.containsKey(pid)) {
                return;
            }
//            CopyOnWriteArrayList<LockReadWrite> list = lockMapList.get(pid);
            List<LockReadWrite> list = lockMapList.get(pid);
            for (LockReadWrite lockReadWrite : list) {
                if (lockReadWrite.tid.equals(tid)) {
                    list.remove(lockReadWrite);
                    logger.log(Level.INFO,"事务id : " + tid.getId() +"  page id : " + pid.getPageNumber() + "释放" +
                            lockReadWrite.lockType.name() + "锁");
                }
            }
            if (list.size() == 0) {
                lockMapList.remove(pid,list);
                logger.log(Level.INFO,"事务id : " + tid.getId() +"  page id : " + pid.getPageNumber() + "释放" +
                        "page锁");
            }
        }
    }
}
