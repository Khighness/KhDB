package top.parak.khdb.server.dm.pagecache;

import top.parak.khdb.common.Error;
import top.parak.khdb.common.Panic;
import top.parak.khdb.server.dm.page.Page;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public interface PageCache {

    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pageNo) throws Exception;

    void close();

    void release(Page page);

    void truncateByPageNo(int maxPageNo);

    int getPageNumber();

    void flushPage(Page page);

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PAGE_SIZE);
    }

}
