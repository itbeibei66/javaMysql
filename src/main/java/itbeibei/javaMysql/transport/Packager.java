package itbeibei.javaMysql.transport;

import itbeibei.javaMysql.MysqlEngine.Server.Executor;

public class Packager {
    private Transporter transporter;
    private Encoder encoder;
    private Executor executor;
    public Packager(Transporter transporter, Encoder encoder, Executor executor) {
        this.transporter = transporter;
        this.encoder = encoder;
        this.executor = executor;
    }

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    public void send2(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transporter.send2(data);
    }

    public Package receive2() throws Exception {
        byte[] data = transporter.receive2();
        if(data == null){
            this.close();
            executor.close();
            return null;
        }
        return encoder.decode(data);
    }

    public Package receive() throws  Exception {
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transporter.close();
    }
}
