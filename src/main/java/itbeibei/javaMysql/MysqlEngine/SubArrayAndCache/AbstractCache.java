package itbeibei.javaMysql.MysqlEngine.SubArrayAndCache;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.dm.page.Page;
import itbeibei.javaMysql.MysqlEngine.dm.page.PageImpl;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }
    protected boolean containsKey(long key) {
        return cache.containsKey(key);
    }

    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                long min = Long.MAX_VALUE;
                long minL = 0;
                for(long l:references.keySet()){
                    if(references.get(l) < min){
                        min = references.get(l);
                        minL = l;
                    }
                }
                releaseOnePage(minL);
                //lock.unlock();
                //throw Error.CacheFullException;
            }
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    protected void releaseAllPage() throws Exception{
        lock.lock();
        pcReleaseAllPage();
        lock.unlock();
    }

    protected abstract void pcReleaseAllPage() throws Exception;

    /**
     * 强行释放一个页面资源, 相比于之前缓存满了直接报错，这种方式更合适一些
     * **/
    protected void releaseOnePage(long key){
        T obj = cache.get(key);
        releaseForCache(obj);
        references.remove(key);
        cache.remove(key);
        count--;
        /*
        Page p = (Page) obj;
        synchronized (p) {
            p.setIsFlushing(false);
        }*/
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            if((!references.containsKey(key)) || (!cache.containsKey(key))){
                return;
            }
            int ref = references.get(key) - 1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
                /*if(Page.class.isInstance(obj)){
                    Page p = (Page) obj;
                    synchronized (p) {
                        p.setIsFlushing(false);
                    }
                }*/

            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                releaseForCache(cache.get(key));
                references.remove(key);
                cache.remove(key);
                count--;
                /*
                if(Page.class.isInstance(cache.get(key))){
                    Page p = (Page) cache.get(key);
                    synchronized (p) {
                        p.setIsFlushing(false);
                    }
                }*/
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
