package simpledb;

import jdk.nashorn.internal.ir.annotations.Immutable;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.Field;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * @author panrixin <panrixin@kuaishou.com>
 * Created on 2022-01-11
 */
public class TestPrx {

    public static void aaa(){
        Random random = new Random();
        int i = random.nextInt(5);
        int count = 6;
        for (int j = 1; j < count ;j ++) {
            System.out.println(random.nextInt(6)+1);
        }
    }
    private static ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    private static int num = 0;
    public static void bbb(){
        System.out.println("加读锁");
        reentrantReadWriteLock.readLock().lock();
        System.out.println(num);
        reentrantReadWriteLock.writeLock().lock();
        System.out.println("加写锁");
        num++;
        reentrantReadWriteLock.writeLock().unlock();
        System.out.println("释放写锁");
        reentrantReadWriteLock.readLock().unlock();
        System.out.println("释放读锁");
    }


    public static <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> els = new HashSet<>();
        els.add(new HashSet<>());
        // Iterator<Set> it;
        // long start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Set<Set<T>> newels = new HashSet<>();
            for (Set<T> s : els) {
                for (T t : v) {
                    Set<T> news = new HashSet<>(s);
                    if (news.add(t))
                        newels.add(news);
                }
            }
            els = newels;
        }

        return els;

    }

    public static void main(String[] args) throws IOException {

        List<Integer> integers = Arrays.asList(1, 2, 3, 4,5);
        Iterator<Integer> iterator = integers.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(iterator.next());
        }


//        long l = System.currentTimeMillis();
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        long l1 = System.currentTimeMillis();
//        System.out.println(l1-l);

//        ConcurrentHashMap<Integer, Integer> concurrentHashMap = new ConcurrentHashMap<>();
//        concurrentHashMap.put(1,1);
//        concurrentHashMap.put(1,2);
//        for (Integer integer : concurrentHashMap.keySet()) {
//            System.out.println(integer + "---" + concurrentHashMap.get(integer));
//        }

//        new Thread(()->bbb()).start();
//        new Thread(()->bbb()).start();

//        ArrayList<Integer> integers = new ArrayList<>();
//        integers.add(1);
//        integers.add(2);
//        integers.add(3);
//        integers.add(4);
//        integers.add(5);
//
//        Predicate<Integer> integerPredicate = s -> {
//            boolean b = integers.contains(s);
//            return b;
//        };
//        integers.stream().filter(integerPredicate).sorted();
//        Set<Set<Integer>> sets = enumerateSubsets(integers, 3);
//        for (Set<Integer> set : sets) {
//            for (Integer integer : set) {
//                System.out.print("[" + integer + " , ");
//            }
//            System.out.println();
//        }
//
//
//        System.out.println(Math.ceil(3.66));
//
//        Queue<Integer> q =  new PriorityQueue<>();
//        q.add(3);
//        q.add(2);
//        q.add(4);
//        for (Integer integer : q) {
//            System.out.println(integer);
//        }
//
//        HashMap<Integer, Double> map = new HashMap<>();
//        double axc = map.get(1);
//        System.out.println(map.get(1));
////        map.put(3,4);
////        map.putIfAbsent(3,4);
//        Integer o = (Integer) map.keySet().toArray()[0];
//        map.keySet().removeIf(key -> key == o);
//        for (Integer integer : map.keySet()) {
//            System.out.println(integer + "--" + map.get(integer));
//        }


//        File file = new File("./test.txt");
//        RandomAccessFile wr = new RandomAccessFile("./test.txt", "rw");
//        byte[] buf = new byte[1024];
//        wr.seek(file.length());
//        wr.write(buf);
//        System.out.println(file.length());
//        wr.seek(file.length() + 1024);
//        wr.write(buf);
//        System.out.println(file.length());
//        byte[] aa = new byte[1024];
//        int read = wr.read(aa, 1024, 1024);
//        System.out.println(aa.toString());
//        System.out.println(file.length());
//
//        String a = "00010001";
//        byte[] bytes = a.getBytes();
//        System.out.println(bytes.length);
//        int aaaa = 17;
//        System.out.println(aaaa | 5);
//        System.out.println(Integer.toHexString(-1));
//        aaa();
//        List<Integer> aaa = new ArrayList<>();
//        aaa.add(0,1);
//        List<Integer> bbb = new ArrayList<>(aaa);
//        bbb.add(4);
//        bbb.add(5);
//        List<Integer> ccc = new ArrayList<>(aaa);
//        ccc.add(3);
//        System.out.println(aaa.hashCode());
//        System.out.println(bbb.hashCode());
//        System.out.println(ccc.hashCode());
//        System.out.println(bbb.size());

        // construct a 3-column table schema
//        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
//        String names[] = new String[]{ "field0", "field1", "field2" };
//        TupleDesc descriptor = new TupleDesc(types, names);
//
//        // create the table, associate it with some_data_file.dat
//        // and tell the catalog about the schema of this table.
//        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
//        Database.getCatalog().addTable(table1, "test");
//
//        // construct the query: we use a simple SeqScan, which spoonfeeds
//        // tuples via its iterator.
//        TransactionId tid = new TransactionId();
//        SeqScan f = new SeqScan(tid, table1.getId());
//
//        try {
//            // and run it
//            f.open();
//            while (f.hasNext()) {
//                Tuple tup = f.next();
//                System.out.println(tup);
//            }
//            f.close();
//            Database.getBufferPool().transactionComplete(tid);
//        } catch (Exception e) {
//            System.out.println ("Exception : " + e);
//        }
//        ArrayList<Integer> objects = new ArrayList<>();
//        objects.add(1);
//        objects.add(2);
//        objects.add(3);
//
//        objects.iterator();
//
//        System.out.println(Math.ceil(337/8));
//        System.out.println(Math.ceil(337/8.0));
//
//        Map<Integer,String> aa = new HashMap<>();
//        String aaa = "asd";
//        aa.compute(1,(s,v)->aaa);
//        String bbb = "nmn";
//        aa.compute(1,(s,v)->bbb);
//        System.out.println(aa.get(1));
//
//        double a = 1.2;
//        System.out.println((int)a);
//
//        int b = 10;
//        System.out.println((10 >> 2) & 0x1);

    }
}
