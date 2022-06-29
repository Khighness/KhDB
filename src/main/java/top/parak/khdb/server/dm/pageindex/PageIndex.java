package top.parak.khdb.server.dm.pageindex;

import top.parak.khdb.server.dm.pagecache.PageCache;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * @author KHighness
 * @since 2022-06-29
 * @email parakovo@gmail.com
 */
public class PageIndex {

    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;



}
