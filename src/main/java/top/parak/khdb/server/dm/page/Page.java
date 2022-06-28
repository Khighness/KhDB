package top.parak.khdb.server.dm.page;

/**
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public interface Page {

    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
