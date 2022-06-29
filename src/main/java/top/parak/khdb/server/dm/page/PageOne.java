package top.parak.khdb.server.dm.page;

import top.parak.khdb.server.dm.pagecache.PageCache;
import top.parak.khdb.toolkit.RandomUtil;

import java.util.Arrays;

/**
 * Page 第一页
 * <p><b>ValidCheck</b></p>
 * <ol>
 * <li>DB启动时，给page的{@code OFFSET_VALID_CHECK ~ OFFSET_VALID_CHECK + LENGTH_VALID_CHECK}字节处填入一个随机字节</li>
 * <li>DB关闭时，将page的{@code OFFSET_VALID_CHECK + LENGTH_VALID_CHECK ~ OFFSET_VALID_CHECK + 2 *LENGTH_VALID_CHECK}字节拷贝到108~115字节</li>
 * <li>用于判断上一次DB是否正常关闭</li>
 * </ol>
 *
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public class PageOne {

    private static final int OFFSET_VALID_CHECK = 100;
    private static final int LENGTH_VALID_CHECK = 8;

    /**
     * 初始化
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 设置page打开
     *
     * @param page page
     */
    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    public static void SetVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    /**
     * 在raw的{@code OFFSET_VALID_CHECK ~ LENGTH_VALID_CHECK}填充一个随机字节
     *
     * @param raw raw
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LENGTH_VALID_CHECK), 0, raw, OFFSET_VALID_CHECK, LENGTH_VALID_CHECK);
    }

    /**
     * 在raw的{@code OFFSET_VALID_CHECK ~ LENGTH_VALID_CHECK}的内容复制到
     * {@code OFFSET_VALID_CHECK + LENGTH_VALID_CHECK ~ OFFSET_VALID_CHECK + 2 * LENGTH_VALID_CHECK}
     *
     * @param raw raw
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OFFSET_VALID_CHECK, raw, OFFSET_VALID_CHECK + LENGTH_VALID_CHECK, LENGTH_VALID_CHECK);
    }

    /**
     * 检验page是否正常关闭
     *
     * @param raw raw
     * @return true代表page正常关闭
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OFFSET_VALID_CHECK, OFFSET_VALID_CHECK + LENGTH_VALID_CHECK),
                Arrays.copyOfRange(raw, OFFSET_VALID_CHECK + LENGTH_VALID_CHECK, OFFSET_VALID_CHECK + 2 * LENGTH_VALID_CHECK));
    }

}
