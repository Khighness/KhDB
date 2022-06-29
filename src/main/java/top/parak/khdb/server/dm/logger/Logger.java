package top.parak.khdb.server.dm.logger;

/**
 * 日志
 *
 * @author KHighness
 * @since 2022-06-29
 * @email parakovo@gmail.com
 */
public interface Logger {

    void log(byte[] data);

    void truncate(long x) throws Exception;

    byte[] next();

    void rewind();

    void close();

}
