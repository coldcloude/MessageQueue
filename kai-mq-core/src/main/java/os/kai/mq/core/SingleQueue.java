package os.kai.mq.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SingleQueue<T> {
    private static class Node<T> {
        private final long index;
        private final T value;
        private final AtomicReference<Node<T>> next = new AtomicReference<>(null);

        private Node(long index,T value) {
            this.index = index;
            this.value = value;
        }
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Condition nonEmpty = lock.writeLock().newCondition();

    private final AtomicReference<Node<T>> head = new AtomicReference<>(null);
    private final AtomicReference<Node<T>> tail = new AtomicReference<>(null);

    private final AtomicLong serial = new AtomicLong(0);
    private final Map<String,Consumer> consumerMap = new ConcurrentHashMap<>();

    public class Consumer {
        private final AtomicReference<Node<T>> current = new AtomicReference<>(null);

        private Consumer(boolean earliest) {
            Node<T> node = earliest?head.get():tail.get();
            current.set(node);
        }

        public T last(){
            Node<T> node = current.get();
            return node==null?null:node.value;
        }

        public T take(long timeout) throws InterruptedException {
            lock.readLock().lock();
            try{
                Node<T> next = current.get().next.get();
                if(next==null){
                    if(timeout!=0){
                        // empty, wait
                        boolean found;
                        if(timeout<0){
                            nonEmpty.await();
                            found = true;
                        }else{
                            found = nonEmpty.await(timeout,TimeUnit.MILLISECONDS);
                        }
                        if(found){
                            next = current.get().next.get();
                        }
                    }
                }
                if(next!=null){
                    //found one, move to next
                    current.set(next);
                    return next.value;
                }else{
                    return null;
                }
            }finally{
                lock.readLock().unlock();
            }
        }

        public List<T> take(long timeout,int max) throws InterruptedException {
            List<T> rst = new ArrayList<T>(max);
            T value = take(timeout);
            if(value!=null){
                rst.add(value);
                for(int i = 0; i<max-1; i++){
                    value = take(0);
                    if(value!=null){
                        rst.add(value);
                    }else{
                        break;
                    }
                }
            }
            return rst;
        }
    }

    public SingleQueue() {
        Node<T> node = new Node<>(serial.getAndIncrement(),null);
        head.set(node);
        tail.set(node);
    }

    public void put(Iterable<T> values) {
        lock.writeLock().lock();
        try{
            //save empty state
            boolean empty = head.get().next.get()==null;
            //add nodes
            Node<T> t = tail.get();
            for(T value : values){
                long index = serial.getAndIncrement();
                Node<T> node = new Node<>(index,value);
                t.next.set(node);
                t = node;
            }
            tail.set(t);
            //emit non empty
            if(empty&&head.get().next.get()!=null){
                nonEmpty.signalAll();
            }
        }finally{
            lock.writeLock().unlock();
        }
    }

    public void clean() {
        lock.writeLock().lock();
        try{
            boolean first = true;
            long min = 0L;
            for(Consumer consumer : consumerMap.values()){
                long i = consumer.current.get().index;
                if(first){
                    first = false;
                    min = i;
                }else if(i-min<0L){
                    min = i;
                }
            }
            if(first){
                //no consumer, clean all
                head.set(tail.get());
            }else{
                //clean to min consumer
                Node<T> curr = head.get();
                while(curr.next.get()!=null&&curr.index-min<0L){
                    curr = curr.next.get();
                }
                head.set(curr);
            }
        }finally{
            lock.writeLock().unlock();
        }
    }

    public void clean(long index) {
        lock.writeLock().lock();
        try{
            //clean to index
            Node<T> curr = head.get();
            while(curr.next.get()!=null&&curr.index-index<0L){
                curr = curr.next.get();
            }
            head.set(curr);
            //reset consumers
            for(Consumer consumer : consumerMap.values()){
                if(consumer.current.get().index-index<0L){
                    consumer.current.set(head.get());
                }
            }
        }finally{
            lock.writeLock().unlock();
        }
    }

    public long getMaxIndex(){
        lock.readLock().lock();
        try{
            return tail.get().index;
        }
        finally{
            lock.readLock().unlock();
        }
    }

    public Consumer getOrCreateConsumer(String name, boolean earliest){
        Consumer consumer = consumerMap.get(name);
        if(consumer==null){
            lock.writeLock().lock();
            try{
                consumer = consumerMap.computeIfAbsent(name,k->new Consumer(earliest));
            }
            finally{
                lock.writeLock().unlock();
            }
        }
        return consumer;
    }

    public void removeConsumer(String name){
        lock.writeLock().lock();
        try{
            Consumer consumer = consumerMap.remove(name);
            if(consumer!=null){
                consumer.current.set(null);
            }
        }
        finally{
            lock.writeLock().unlock();
        }
    }
}
