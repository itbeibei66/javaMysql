package itbeibei.javaMysql.transport;

import itbeibei.javaMysql.MysqlEngine.Server.Server;
import itbeibei.javaMysql.MysqlEngine.Server.ServerSocket;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private SocketChannel socketChannel;
    /*发送数据缓冲区*/
    private ByteBuffer sBuffer = ByteBuffer.allocate(1024);
    /*接受数据缓冲区*/
    private ByteBuffer rBuffer = ByteBuffer.allocate(1024);
    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.socketChannel = socket.getChannel();
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }


    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();

    }

    public void send2(byte[] data) throws IOException, DecoderException {
        sBuffer.clear();
        sBuffer.put(data);
        sBuffer.flip();
        socketChannel.write(sBuffer);
    }

    public byte[] receive2() throws IOException, DecoderException {
            rBuffer.clear();
            int len = socketChannel.read(rBuffer);
            if(len > 0){
                byte[] bs = new byte[len];
                rBuffer.flip();
                rBuffer.get(bs);
                return bs;
            }else if(len == -1) {
                InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
                String s = address.getAddress().getHostAddress()+":"+address.getPort();
                System.out.println("close connection: " + s);
                ServerSocket.NumbersOfClient.getAndDecrement();
            }
            return null;
    }

    public byte[] receive() throws Exception {
        String line = reader.readLine();

        if(line == null) {
            close();
            return null;
        }

        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true)+"\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
