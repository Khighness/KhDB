package top.parak.khdb.server.dm.logger;

import com.google.common.primitives.Bytes;
import top.parak.khdb.common.Error;
import top.parak.khdb.common.Panic;
import top.parak.khdb.toolkit.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>日志文件格式: [XCheckSum] [Log1] [Log2] ... [LogN] [BadTail]</p>
 * <ol>
 * <li>XCheckSum: int类型，对后续所有日志计算的校验和</li>
 * <li>BadTail: 在数据库崩溃时，没有来得及写完的日志数据，不一定存在</li>
 * </ol>
 * <p>
 * 每条日志格式：[Size] [CheckSum] [Data]
 * </p>
 *
 * <pre>
 * +----------------+
 * |     4 byte     |
 * +----------------+
 * |    XCheckSum   |
 * +----------------+----------------+----------------+
 * |     4 byte     |     4 byte     |     4 byte     |
 * +----------------+----------------+----------------+
 * |      Size      |     CheckSum   |      Data      |
 * +----------------+----------------+----------------+
 * |      Size      |     CheckSum   |      Data      |
 * +----------------+----------------+----------------+
 * |      ...                                         |
 * +----------------+----------------+----------------+
 * |     4 byte     |
 * +----------------+
 * |     BadTail    |
 * +----------------+
 * </pre>
 *
 * @author KHighness
 * @since 2022-06-29
 * @email parakovo@gmail.com
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;
    private static final int OFFSET_SIZE = 0;
    private static final int OFFSET_CHECK_SUM = OFFSET_SIZE + 4;
    private static final int OFFSET_DATA= OFFSET_CHECK_SUM + 4;
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private Lock lock;

    private long position;
    private long fileSize;
    private int xCheckSum;

    LoggerImpl(RandomAccessFile file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        this.lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile file, FileChannel fileChannel, int xCheckSum) {
        this.file = file;
        this.fileChannel = fileChannel;
        this.lock = new ReentrantLock();
        this.xCheckSum = xCheckSum;
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fileChannel.position(0);
            fileChannel.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());

        this.fileSize = size;
        this.xCheckSum = xCheckSum;
        checkAndRemoveTail();
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fileChannel.position(fileChannel.size());
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXCheckSum(log);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OFFSET_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            file.close();
            fileChannel.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            xCheck = calCheckSum(xCheck, log);
        }
        if (xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private byte[] internNext() {
        if (position + OFFSET_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fileChannel.position(position);
            fileChannel.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OFFSET_DATA > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OFFSET_DATA + size);
        try {
            fileChannel.position(position);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int calCheckSum = calCheckSum(0, Arrays.copyOfRange(log, OFFSET_DATA, log.length));
        int fileCheckSum = Parser.parseInt(Arrays.copyOfRange(log, OFFSET_CHECK_SUM, OFFSET_DATA));
        if (calCheckSum != fileCheckSum) {
            return null;
        }
        position += log.length;
        return log;
    }

    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checkSum, data);
    }

    private void updateXCheckSum(byte[] log) {
        this.xCheckSum = calCheckSum(this.xCheckSum, log);
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
