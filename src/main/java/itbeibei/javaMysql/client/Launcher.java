package itbeibei.javaMysql.client;



import itbeibei.javaMysql.transport.Encoder;
import itbeibei.javaMysql.transport.Packager;
import itbeibei.javaMysql.transport.Transporter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;


public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 6001);
        Transporter t = new Transporter(socket);
        /**
        InetSocketAddress Server = new InetSocketAddress("127.0.0.1", 6001);
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        socketChannel.connect(Server);
        Transporter t = new Transporter(socketChannel.socket());
**/

        Encoder e = new Encoder();

        Packager packager = new Packager(t, e);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
