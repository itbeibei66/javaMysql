package itbeibei.javaMysql.MysqlEngine;

import itbeibei.javaMysql.Error.Error;
import itbeibei.javaMysql.MysqlEngine.Server.Server;
import itbeibei.javaMysql.MysqlEngine.Server.ServerSocket;
import itbeibei.javaMysql.MysqlEngine.dm.DataManager;
import itbeibei.javaMysql.MysqlEngine.tbm.TableManager;
import itbeibei.javaMysql.MysqlEngine.tm.TransactionManager;
import itbeibei.javaMysql.MysqlEngine.utils.Panic;
import itbeibei.javaMysql.MysqlEngine.vm.VersionManager;
import itbeibei.javaMysql.MysqlEngine.vm.VersionManagerImpl;
import org.apache.commons.cli.*;

import java.io.IOException;

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
        }else if(method.equals("open")){
            //1<<17表示128kb，即使用128kb的空间
            System.out.println("Usage: launcher (open) DBPath");
            openDB(address,1<<17);
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
