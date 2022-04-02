package itbeibei.javaMysql.MysqlEngine.tm;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin();                    //开启一个新事务
    void commit(long xid);           //提交一个事务
    void abort(long xid);            //取消一个事务
    boolean isActive(long xid);      //查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long xid);   //查询一个事务的状态是否是已提交
    boolean isAborted(long xid);     //查寻一个事务的状态是否是已取消
    void close();                    //关闭TM


    //创建XID文件
    public static TransactionManagerImpl create(String path) {
        //新建一个XID文件
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        //判断其读写性
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        //针对此文件定义的文件操作类及管道
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            //初始化文件操作类
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头(XID文件头为8个字节)
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            //移动到要写入的位置
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //新建事务管理器，并将XID文件的操作类及管道传递进去
        return new TransactionManagerImpl(raf, fc);
    }
    //打开XID文件
    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        //判断文件的读写性
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        //与上个方法相同
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
