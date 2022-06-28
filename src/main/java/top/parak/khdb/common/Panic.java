package top.parak.khdb.common;

/**
 * @author KHighness
 * @since 2022-06-28
 * @email parakovo@gmail.com
 */
public class Panic {

    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(1);
    }

}
