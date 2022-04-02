package itbeibei.javaMysql.MysqlEngine.dm.dataItem;

import com.google.common.primitives.Bytes;
import itbeibei.javaMysql.MysqlEngine.SubArrayAndCache.SubArray;
import itbeibei.javaMysql.MysqlEngine.dm.DataManagerImpl;
import itbeibei.javaMysql.MysqlEngine.dm.page.Page;
import itbeibei.javaMysql.MysqlEngine.utils.Parser;
import itbeibei.javaMysql.MysqlEngine.utils.Types;

import java.util.Arrays;
//db中数据的格式，0位是有效位，1-2位是数据长度，后面是数据(其中数据库数据前8字节记录Xmin事务id，随后8字节记录Xmax的id，之后记录数据)
public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
