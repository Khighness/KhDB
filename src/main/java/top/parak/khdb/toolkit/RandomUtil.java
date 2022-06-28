package top.parak.khdb.toolkit;

import java.security.SecureRandom;

/**
 * @author KHighness
 * @since 2022-06-29
 * @email parakovo@gmail.com
 */
public class RandomUtil {

    public static byte[] randomBytes(int length) {
        SecureRandom sr = new SecureRandom();
        byte[] buf = new byte[length];
        sr.nextBytes(buf);
        return buf;
    }

}
