package itbeibei.javaMysql.MysqlEngine.vm;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.SubArrayAndCache.AbstractCache;
import itbeibei.javaMysql.MysqlEngine.dm.DataManager;
import itbeibei.javaMysql.MysqlEngine.dm.page.Page;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManager;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManagerImpl;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
//VM管理器，向上层提供read，insert，delete方法，继承引用计数缓存类
@SuppressWarnings("All")
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{
    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }
    @Override
    protected void pcReleaseAllPage() {

    }
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.DeadlockException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                if(!Visibility.isVisible(tm, t, entry)) {
                    return false;
                }
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry) || Visibility.isActiveTransactionDeleteIt(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            /*
            while (true) {
                Page pg = entry.getDataItem().page();
                boolean isFlushing;
                synchronized (pg) {
                    isFlushing = pg.getIsFlushing();
                }
                if(isFlushing || !dm.containsKey(pg.getPageNumber())){
                    entry.release();
                    delete(xid,uid);
                    //throw Error.PageIsFlushNow;
                }else{
                    break;
                }
            }*/

            entry.setXmax(xid);
            //lt.remove(xid, uid);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public byte[] SuperRead(long uid) throws Exception {
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            return entry.allData();
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    @Override
    public long getMinActiveTransaction() {
        long res = Long.MAX_VALUE;
        for(long a : activeTransaction.keySet()){
            if(a!=0){
                res = Math.min(res , a);
            }
        }
        if(res == Long.MAX_VALUE) {
            res = tm.getXidCounter()+1;
        }
        return res;
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }
    @Override
    public boolean isCommited(long xid){
        return tm.isCommitted(xid);
    }

    @Override
    public boolean isAborted(long xid){
        return tm.isAborted(xid);
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
