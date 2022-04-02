package itbeibei.javaMysql.MysqlEngine.vm;

import itbeibei.javaMysql.MysqlEngine.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;
//事务类实体，包含事务id，事务隔离级别，活跃事务快照，错误，
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }
    //超级事务永远是Commited，不在快照中
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
