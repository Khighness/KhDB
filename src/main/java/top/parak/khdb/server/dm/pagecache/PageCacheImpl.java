package top.parak.khdb.server.dm.pagecache;

import top.parak.khdb.common.Error;
import top.parak.khdb.common.Panic;
import top.parak.khdb.server.common.AbstractCache;
import top.parak.khdb.server.dm.page.Page;
import top.parak.khdb.server.dm.page.PageImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LTM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private Lock fileLock;
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel,int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LTM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fileChannel = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        return get(pageNo);
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByPageNo(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int) key;
        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pageNo, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    private static long pageOffset(int pageNo) {
        return (pageNo - 1) * PAGE_SIZE;
    }

}
