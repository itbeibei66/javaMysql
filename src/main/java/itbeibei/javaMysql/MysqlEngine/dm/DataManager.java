package itbeibei.javaMysql.MysqlEngine.dm;

import itbeibei.javaMysql.MysqlEngine.dm.dataItem.DataItem;
import itbeibei.javaMysql.MysqlEngine.dm.logger.Logger;
import itbeibei.javaMysql.MysqlEngine.dm.page.PageOne;
import itbeibei.javaMysql.MysqlEngine.dm.pageCache.PageCache;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();
    void setDataItemInvalid(long uid) throws Exception;
    void flushAllPage() throws Exception;
    //创建一个DM，要知道DM包含什么，它包含一个页面缓存器，一个日志
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
