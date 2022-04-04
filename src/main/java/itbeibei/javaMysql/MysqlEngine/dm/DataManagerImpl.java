package itbeibei.javaMysql.MysqlEngine.dm;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.SubArrayAndCache.AbstractCache;
import itbeibei.javaMysql.MysqlEngine.dm.dataItem.DataItem;
import itbeibei.javaMysql.MysqlEngine.dm.dataItem.DataItemImpl;
import itbeibei.javaMysql.MysqlEngine.dm.logger.Logger;
import itbeibei.javaMysql.MysqlEngine.dm.page.Page;
import itbeibei.javaMysql.MysqlEngine.dm.page.PageOne;
import itbeibei.javaMysql.MysqlEngine.dm.page.PageX;
import itbeibei.javaMysql.MysqlEngine.dm.pageCache.PageCache;
import itbeibei.javaMysql.MysqlEngine.dm.pageIndex.PageIndex;
import itbeibei.javaMysql.MysqlEngine.dm.pageIndex.PageInfo;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManager;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManagerImpl;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;
import itbeibei.javaMysql.MysqlEngine.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    //用来选择插入哪个页面的管理器
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }
    @Override
    public void setDataItemInvalid(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        di.before();
        try {
            di.setDataItemRawInvalid();
        }
        finally {
            di.after(TransactionManagerImpl.SUPER_XID);
            di.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }
    @Override
    public void flushAllPage() throws Exception{
        pc.flushAllPage();
    }
    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        //创建第一页,并写入到磁盘中
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}
