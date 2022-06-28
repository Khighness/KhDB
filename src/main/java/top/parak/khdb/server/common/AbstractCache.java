package top.parak.khdb.server.common;

import top.parak.khdb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 引用计数缓存
 *
 * @author KHighness
 * @since 2022-06-14
 * @email parakovo@gmail.com
 */
public abstract class AbstractCache<T> {

    /**
     * 实际缓存的数量
     */
    private HashMap<Long, T> cache;
    /**
     * 元素的引用个数
     */
    private HashMap<Long, Integer> refrences;
    /**
     * 正在被获取的资源
     */
    private HashMap<Long, Boolean> getting;

    /**
     * 最大缓存资源数量
     */
    private int maxResource;
    /**
     * 缓存中元素的个数
     */
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        refrences = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();

            // 其他线程正在获取资源
            // 等待一段时间重新尝试
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    continue;
                }
                continue;
            }

            // 尝试在缓存中获取
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                refrences.put(key, refrences.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 资源不在缓存中，缓存已满
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 资源不在缓存中，放入缓存
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        refrences.put(key, 1);
        lock.unlock();
        return obj;
    }

    protected void release(long key) {
        lock.lock();
        try {
            int ref = refrences.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                refrences.remove(key);
                cache.remove(key);
                count--;
            } else {
                refrences.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                refrences.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract T getForCache(long key) throws Exception;

    protected abstract void releaseForCache(T obj);

}
