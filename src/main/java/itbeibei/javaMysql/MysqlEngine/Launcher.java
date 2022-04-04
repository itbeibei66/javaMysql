package itbeibei.javaMysql.MysqlEngine;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.Server.Server;
import itbeibei.javaMysql.MysqlEngine.Server.ServerSocket;
import itbeibei.javaMysql.MysqlEngine.dm.DataManager;
import itbeibei.javaMysql.MysqlEngine.tbm.TableManager;
import itbeibei.javaMysql.MysqlEngine.tbm.TableManagerImpl;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManager;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;
import itbeibei.javaMysql.MysqlEngine.vm.VersionManager;
import itbeibei.javaMysql.MysqlEngine.vm.VersionManagerImpl;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Launcher {
    public static final int port = 6001;
    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;
    public static final boolean flags = false;
    public static void main(String[] args) throws ParseException, IOException {
        if(args == null || args.length!=2){
            Panic.panic(Error.NoArguments);
            return;
        }
        String method = args[0];
        String address = args[1];
        if(method.equals("create") && flags){
            createDB(address);
            System.out.println("Usage: launcher (create) DBPath");
        }else if(!flags || method.equals("open")){
            //1<<17表示1MB，即使用1MB的空间
            System.out.println("Usage: launcher (open) DBPath");
            openDB(address,1<<20);
        }else{
            Panic.panic(Error.InvalidArgumentsException);
        }

    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) throws IOException {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        Runnable r= new Clear(tbm);
        Thread t = new Thread(r,"t1");
        t.start();
        new ServerSocket(port,tbm);
        //new Server(port, tbm).start();
        System.out.println("The DB has been closed");
    }



    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
class Clear implements Runnable {
    private TableManager tbm;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public Clear(TableManager tbm) {
        this.tbm = tbm;
    }
    private long time = 1000*60*60;
    @Override
    public void run() {
        try {
            System.out.println("the clear thread is going to work");
            while(true){
                Thread.sleep(time);
                Date date = new Date();
                String format = simpleDateFormat.format(date);
                System.out.println("the clear and flush time: "+ format);
                System.out.println("Clear the DeprecatedData, Clear the BPlusTree");
                tbm.deleteDeprecatedData();
                System.out.println("Clear over");
                System.out.println("flushAllPage at Now");
                tbm.flushAllPage();
                System.out.println("flushAllPage over");
                System.out.println("the next clear and flush will be in "+ time/1000 +" seconds");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
