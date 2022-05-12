package itbeibei.javaMysql.client;

import itbeibei.javaMysql.transport.Package;
import itbeibei.javaMysql.transport.Packager;

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send2(pkg);
        return packager.receive2();
    }
    public Package roundTrip2(Package pkg) throws  Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
