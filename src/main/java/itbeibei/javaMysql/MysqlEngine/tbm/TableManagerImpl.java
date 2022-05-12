package itbeibei.javaMysql.MysqlEngine.tbm;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.Parser.statement.*;
import itbeibei.javaMysql.MysqlEngine.dm.DataManager;
import itbeibei.javaMysql.MysqlEngine.utils.Parser;
import itbeibei.javaMysql.MysqlEngine.vm.VersionManager;
import itbeibei.javaMysql.MysqlEngine.vm.VersionManagerImpl;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();

        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) throws Exception {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("数据库中的表如下：").append("\n");
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if(t == null) {
                return sb.toString().getBytes();
            }
            sb.append("当前事务所创建的表如下：").append("\n");
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }


    }
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();


    }
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();


    }
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }

    @Override
    public Map<String,List<Long>> readAllKeyUid() throws Exception {
        Map<String, List<Long>> res = new HashMap<>();
        Set<String> tbName = tableCache.keySet();
        List<Table> tables = new ArrayList<>();
        lock.lock();
        for(String name : tbName){
            tables.add(tableCache.get(name));
        }
        lock.unlock();
        for(Table tb : tables){
            res.put(tb.name, tb.readAllKeyUid());
        }
        return res;
    }

    @Override
    public void deleteDeprecatedData() throws Exception {
        Map<String, List<Long>> map = readAllKeyUid();
        long minActiveTransaction = vm.getMinActiveTransaction();
        for(String tbName : map.keySet()){
            lock.lock();
            Table table = tableCache.get(tbName);
            lock.unlock();
            for(Long u : map.get(tbName)){
                long[] res = table.readOneUidX(u);
                long l1 = res[0]; long l2 = res[1];
                if( vm.isAborted(l1) || (l2 != 0 && vm.isCommited(l2) && l2 < minActiveTransaction) ) {
                    try{
                        dm.setDataItemInvalid(u);
                        table.delete(u);
                    }catch (Exception e){
                        throw e;
                    }
                }
            }
        }
    }
    @Override
    public void flushAllPage() throws Exception{

        dm.flushAllPage();

    }
}
