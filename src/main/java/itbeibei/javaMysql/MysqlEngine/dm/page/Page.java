package itbeibei.javaMysql.MysqlEngine.dm.page;
//页面接口，需要包含获取页号，设置脏页，获取数据的方法
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
