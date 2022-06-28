package top.parak.khdb.server.common;

/**
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public class SubArray {

    public byte[] raw;
    private int start;
    private int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }

}
