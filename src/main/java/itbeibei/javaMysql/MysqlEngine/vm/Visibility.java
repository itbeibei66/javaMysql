package itbeibei.javaMysql.MysqlEngine.vm;

import itbeibei.javaMysql.MysqlEngine.tm.TransactionManager;

public class Visibility {
    //版本跳跃问题，只有可重复读的情况下会产生版本跳跃
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            //删除数据的事务已提交而且其版本号大于当前事务id或者其在自己的活跃快照中
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }
    //level = 0表示读已提交，level = 1表示可重复读
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //数据是由自己创建的，而且未被删除，必然看得见
        if(xmin == xid && xmax == 0) return true;
        //创建数据的事务已经提交了
        if(tm.isCommitted(xmin)) {
            //数据未被删除，可以看见
            if(xmax == 0) return true;
            if(xmax != xid) {
                //删除数据的事务未提交，可以看见
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        //其他情况都看不见
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //数据是由自己创建的，而且未被删除，必然看得见
        if(xmin == xid && xmax == 0) return true;
        //创建数据的事务已经提交，而且创建数据的事务版本号比自己小，以及该事务不在自己的活跃快照中
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            //如果自己没有删除数据
            if(xmax != xid) {
                //删除数据的事务未提交，删除数据的事务提交了但是其版本号比自己大，删除数据的事务提交了而且版本号比自己小，但是它在自身的活跃快照中
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
