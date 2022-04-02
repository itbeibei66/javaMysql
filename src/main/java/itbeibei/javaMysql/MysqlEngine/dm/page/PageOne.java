package itbeibei.javaMysql.MysqlEngine.dm.page;

import itbeibei.javaMysql.MysqlEngine.dm.pageCache.PageCache;
import itbeibei.javaMysql.MysqlEngine.utils.RandomUtil;

import java.util.Arrays;
/**
 * 数据文件第一页，在开启时，需要对其100-107字节处设置随机数，在关闭时，将100-107的数据复制到108-115字节处
 * 下一次打开该文件时，则需要检查二者是否相等
 * **/
public class PageOne {
    //数据文件第一页的起始标记点
    private static final int OF_VC = 100;
    //数据文件第一页的起始标记长度
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
