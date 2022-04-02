package itbeibei.javaMysql.client;


import itbeibei.javaMysql.MysqlEngine.Server.Executor;
import itbeibei.javaMysql.transport.Encoder;
import itbeibei.javaMysql.transport.Packager;
import itbeibei.javaMysql.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;


public class Launcher {
    public static void main(String[] args) throws IOException {
        //Socket socket = new Socket("127.0.0.1", 6001);
        InetSocketAddress Server = new InetSocketAddress("127.0.0.1", 6001);
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        socketChannel.connect(Server);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socketChannel.socket());
        Packager packager = new Packager(t, e);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
