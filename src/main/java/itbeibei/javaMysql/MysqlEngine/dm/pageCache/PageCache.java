package itbeibei.javaMysql.MysqlEngine.dm.pageCache;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.dm.page.Page;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    //每页大小为8KB
    public static final int PAGE_SIZE = 1 << 13;
    //新建页面，将页面刷盘，并返回页面编号
    int newPage(byte[] initData);
    //根据页面编号获得该页面
    Page getPage(int pgno) throws Exception;
    //关闭缓存资源
    void close();
    //强行释放一个缓存
    void release(Page page);
    //设置页面数量，填充文件长度
    void truncateByBgno(int maxPgno);
    //获得页面编号
    int getPageNumber();
    //将页面刷盘
    void flushPage(Page pg);
    //强制刷盘
    void flushAllPage() throws Exception;
    //memory表示传入的内存大小，以此来设置最大缓存资源数
    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
