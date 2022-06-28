package top.parak.khdb.server.dm.page;

import top.parak.khdb.server.dm.pagecache.PageCache;
import top.parak.khdb.toolkit.RandomUtil;

import java.util.Arrays;

/**
 * Page No.1
 * <p>
 * ValidCheck
 * DB启动时给100~107字节处填入一个随机字节，DB管理时将其拷贝到108~115字节，
 * 用于判断上一次DB是否正常关闭。
 * </p>
 *
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public class PageOne {

    private static final int OF_VC  = 100;
    private static final int LEN_VC = 8;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void SetVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC, OF_VC + 2 * LEN_VC));
    }

}
