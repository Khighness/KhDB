package top.parak.khdb.common;

/**
 * @author KHighness
 * @since 2022-06-15
 * @email parakovo@gmail.com
 */
public class Error {

    /*=============================================== common ===============================================*/
    public static final Exception CacheFullException = new RuntimeException("cache is full");
    public static final Exception FileExistsException = new RuntimeException("file already exists");
    public static final Exception FileNotExistsException = new RuntimeException("file does not exist");
    public static final Exception FileCannotRWException = new RuntimeException("file cannot read or write");

    /*================================================= dm =================================================*/
    public static final Exception BadLogException = new RuntimeException("Bad log file");
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small");
    public static final Exception DataTooLargeException = new RuntimeException("Data too large");
    public static final Exception DatabaseBusyException = new RuntimeException("Database is busy");





}
