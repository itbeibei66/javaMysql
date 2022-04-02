package itbeibei.javaMysql.MysqlEngine.Server;

import itbeibei.javaMysql.MysqlEngine.tbm.TableManager;
import itbeibei.javaMysql.transport.Encoder;
import itbeibei.javaMysql.transport.Package;
import itbeibei.javaMysql.transport.Packager;
import itbeibei.javaMysql.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {
    private int port;
    TableManager tbm;
    public static AtomicInteger NumbersOfClient = new AtomicInteger(0);
    public static boolean flag = false;
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }
    @Deprecated
    public void start2() throws IOException {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.socket().bind(new InetSocketAddress(port));
        serverSocket.configureBlocking(false);
        Selector selector=Selector.open();
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("Server listen to port: " + port);
        try{
            Packager packager = null;
            Encoder e = new Encoder();

            while(true){
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator=selectionKeys.iterator();
                while(iterator.hasNext()){
                    SelectionKey next = iterator.next();
                    if(next.isAcceptable()){
                        ServerSocketChannel server = (ServerSocketChannel) next.channel();
                        SocketChannel socketChannel = server.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector,SelectionKey.OP_READ);
                        NumbersOfClient.getAndIncrement();
                        InetSocketAddress address = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
                        String s = address.getAddress().getHostAddress()+":"+address.getPort();
                        System.out.println("Establish connection: " + s);
                        System.out.println("The Total NumbersOfClients at Now: "+Server.NumbersOfClient.get());
                    }else if(next.isReadable()){
                        SocketChannel channel = (SocketChannel) next.channel();
                        Transporter t = new Transporter(channel.socket());
                        Executor exe = new Executor(tbm);
                        packager = new Packager(t, e, exe);
                        Package pkg;
                        try {
                            pkg = packager.receive2();
                        } catch(Exception e2) {
                            break;
                        }
                        if(pkg == null){
                            iterator.remove();
                            continue;
                        }
                        byte[] sql = pkg.getData();
                        byte[] result = null;
                        Exception e3 = null;
                        try {
                            result = exe.execute(sql);
                        } catch (Exception e1) {
                            e3 = e1;
                            e3.printStackTrace();
                        }
                        pkg = new Package(result, e3);
                        try {
                            packager.send2(pkg);
                        } catch (Exception e1) {
                            e1.printStackTrace();
                            break;
                        }
                    }
                    iterator.remove();
                }
            }
        }catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
        }


    }
    public void start() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                Socket socket = ss.accept();
                if(flag){
                    return;
                }
                NumbersOfClient.getAndIncrement();
                InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
                String s = address.getAddress().getHostAddress()+":"+address.getPort();
                System.out.println("Establish connection: " + s);
                System.out.println("The Total NumbersOfClients at Now: "+Server.NumbersOfClient.get());
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }
}

class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        Packager packager = null;
        Executor exe = new Executor(tbm);
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e ,exe);
        } catch(Exception e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Server.NumbersOfClient.getAndDecrement();
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        String s = address.getAddress().getHostAddress()+":"+address.getPort();
        System.out.println(s+"has been closed");

    }
}
