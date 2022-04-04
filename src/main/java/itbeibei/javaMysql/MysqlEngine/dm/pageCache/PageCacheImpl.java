package itbeibei.javaMysql.MysqlEngine.dm.pageCache;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.SubArrayAndCache.AbstractCache;
import itbeibei.javaMysql.MysqlEngine.dm.page.Page;
import itbeibei.javaMysql.MysqlEngine.dm.page.PageImpl;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    //缓存容量的最小值
    private static final int MEM_MIN_LIM = 10;
    //文件名后缀
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    //记录当前打开数据库文件的页数
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }
    //新建页面（根据指定的字节数组）
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }
    //根据页面编号从缓存中返回一个页面
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 当缓存中没有，就根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }
    //页面被驱逐，是脏页就刷盘
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }
    //释放一个页面资源
    public void release(Page page) {
        release((long)page.getPageNumber());
    }
    //刷盘
    public void flushPage(Page pg) {
        flush(pg);
    }
    //强制刷盘
    @Override
    public void flushAllPage() throws Exception {
        for(int i = 2; i <= pageNumbers.get(); i++) {
            Page pg = getPage(i);
            releaseForCache(pg);
        }
    }
    //刷盘具体操作
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            if(!pg.isDirty()){
                return;
            }
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
            System.out.println("刷盘成功");
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
    //根据maxPgno修改数据文件最大长度并更新数据库文件页数
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }
    //关闭资源操作
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }
}
