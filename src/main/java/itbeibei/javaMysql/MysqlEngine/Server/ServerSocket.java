package itbeibei.javaMysql.MysqlEngine.Server;

import itbeibei.javaMysql.MysqlEngine.tbm.TableManager;
import itbeibei.javaMysql.transport.Encoder;
import itbeibei.javaMysql.transport.Package;
import itbeibei.javaMysql.transport.Packager;
import itbeibei.javaMysql.transport.Transporter;


import java.io.IOException;

import java.net.InetSocketAddress;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class ServerSocket {
    private int port;
    private TableManager tbm;
    public static AtomicInteger NumbersOfClient = new AtomicInteger(0);
    //解码buffer
    // private CharsetDecoder decode = Charset.forName("UTF-8").newDecoder();

    /*映射客户端channel */

    private Map<String, SocketChannel> clientsMap = new HashMap<String, SocketChannel>();

    private Selector selector;
    private Map<String, Transporter> transporterMap = new HashMap<>();
    private Map<String, Packager> packagerMap = new HashMap<>();
    private Map<String, Executor> executorMap = new HashMap<>();
    private int i=0;
    private Encoder e = new Encoder();
    private ThreadPoolExecutor tpe;
    public ServerSocket(int port, TableManager tbm){
        this.port = port;
        this.tbm = tbm;
        this.tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            init();
            listen();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void init() throws Exception{
        /*
         *启动服务器端，配置为非阻塞，绑定端口，注册accept事件
         *ACCEPT事件：当服务端收到客户端连接请求时，触发该事件
         */
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        java.net.ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(port));
        selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("server start on port:"+port);
    }

    /**
     * 服务器端轮询监听，select方法会一直阻塞直到有相关事件发生或超时
     */
    private void listen(){

        while (true) {
            try {
                selector.select();//返回值为本次触发的事件数
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                for(SelectionKey key : selectionKeys){
                    handle(key);
                }
                selectionKeys.clear();//清除处理过的事件
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }


    /**
     * 处理不同的事件
     */
    public void handle(SelectionKey selectionKey) throws Exception {

        ServerSocketChannel server;
        SocketChannel client;

        if (selectionKey.isAcceptable()) {
            /*
             * 客户端请求连接事件
             * serversocket为该客户端建立socket连接，将此socket注册READ事件，监听客户端输入
             * READ事件：当客户端发来数据，并已被服务器控制线程正确读取时，触发该事件
             */
            server = (ServerSocketChannel) selectionKey.channel();
            client = server.accept();
            client.configureBlocking(false);
            String address0 = client.getLocalAddress().toString();

            Transporter t = new Transporter(client.socket());
            Executor exe = new Executor(tbm);
            Packager packager = new Packager(t, e, exe);
            clientsMap.put(client.getLocalAddress().toString().substring(1)+ i++, client);

            client.register(selector, SelectionKey.OP_READ);
            NumbersOfClient.getAndIncrement();
            InetSocketAddress address = (InetSocketAddress)client.socket().getRemoteSocketAddress();
            String s = address.getAddress().getHostAddress()+":"+address.getPort();
            transporterMap.put(s,t);
            executorMap.put(s,exe);
            packagerMap.put(s,packager);
            System.out.println("Establish connection: " + s);
            System.out.println("The Total NumbersOfClients at Now: "+ NumbersOfClient.get());
        } else if (selectionKey.isReadable()) {
            /*
             * READ事件，收到客户端发送数据，读取数据后继续注册监听客户端
             */
            client = (SocketChannel) selectionKey.channel();
            InetSocketAddress address = (InetSocketAddress)client.socket().getRemoteSocketAddress();
            String s = address.getAddress().getHostAddress()+":"+address.getPort();
            Transporter t = transporterMap.get(s);
            Executor exe = executorMap.get(s);
            Packager packager = packagerMap.get(s);

            Package pkg = null;
            try {
                pkg = packager.receive2();
            } catch(Exception e2) {

            }
            if(pkg == null){
                return;
            }
            Callable<Package> r = new Handler(exe, pkg, packager);
            FutureTask<Package> f = new FutureTask<>(r);
            tpe.submit(f);
            client.register(selector, SelectionKey.OP_READ);


        }


    }
}
class Handler implements Callable<Package> {
    private Executor exe;
    private Package pkg;
    private Packager packager;
    public Handler(Executor exe, Package pkg, Packager packager) {
       this.exe = exe;
       this.pkg = pkg;
       this.packager = packager;
    }


    @Override
    public Package call() {
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
        }
        return null;
    }
}
