package itbeibei.javaMysql.MysqlEngine.vm;

import itbeibei.javaMysql.Error.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
//死锁检测表
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表

    private Map<Long, List<Long>> waitU;      // XID正在等待的UID列表
    private Lock lock;
    private Map<Long, Lock> u2l;
    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();

        waitU = new HashMap<>();
        lock = new ReentrantLock();
        u2l = new HashMap<>();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                if(!u2l.containsKey(uid)) u2l.put(uid, new ReentrantLock());
                u2l.get(uid).lock();
                return null;
            }
            putIntoList(waitU, xid, uid);
            putIntoList(wait, uid, xid);//xid ,uid 修改，原来未xid，uid，调换了顺序
            if(hasDeadLock()) {
                removeFromList(waitU, xid, uid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            Lock l = u2l.get(uid);
            lock.unlock();
            l.lock();
            lock.lock();
            u2x.put(uid, xid);
            putIntoList(x2u, xid, uid);
            removeFromList(waitU, xid, uid);
            removeFromList(wait, uid, xid);
            return l;

        } finally {
            lock.unlock();
        }
    }

    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            waitU.remove(xid);
            x2u.remove(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    u2x.remove(uid);
                    u2l.get(uid).unlock();
                }
            }
        } finally {
            lock.unlock();
        }
    }
    public void remove(long xid, long uid) {
        lock.lock();
        try {
            removeFromList(x2u, xid, uid);
            u2x.remove(uid);
            u2l.get(uid).unlock();
        } finally {
            lock.unlock();
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);
        boolean res = false;
        List<Long> uids = waitU.get(xid);
        if(uids == null) return false;
        for(Long uid : uids) {
            Long x = u2x.get(uid);
            assert x != null;
            res = res || dfs(x);
        }
        return res;

    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }
}
