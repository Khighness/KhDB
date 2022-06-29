package top.parak.khdb.server.dm.page;

import top.parak.khdb.server.dm.pagecache.PageCache;
import top.parak.khdb.toolkit.Parser;

import java.util.Arrays;

/**
 * Page 普通页
 * <p><b>FreeSpaceOffset</b></p>
 * <p>FSO：空闲位置的偏移量</p>
 * <p>写入page之前获取FSO，确定写入的位置，写入之后更新FSO</p>
 *
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public class PageX {

    private static final short OFFSET_FREE = 0;
    private static final short OFFSET_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_DATA;

    /**
     * 初始化
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OFFSET_DATA);
        return raw;
    }

    /**
     * 获取page的FSO
     *
     * @param page page
     * @return FSO
     */
    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    /**
     * 向raw插入page
     *
     * @param page page
     * @param raw  raw
     * @return 插入位置
     */
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 获取page的空闲空间大小
     *
     * @param page page
     * @return 空闲空间大小
     */
    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int) getFSO(page.getData());
    }

    /**
     * 江raw插入page中的offset位置，并更新page的FSO
     *
     * @param page   page
     * @param raw    raw
     * @param offset offset
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 江raw插入page中的offset位置，不更新page的FSO
     *
     * @param page   page
     * @param raw    raw
     * @param offset offset
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }

    /**
     * 设置FSO
     *
     * @param raw    raw
     * @param ofData free space offset byes
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OFFSET_FREE, OFFSET_DATA);
    }

    /**
     * 获取FSO
     *
     * @param raw   raw
     * @return free space offset
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

}
