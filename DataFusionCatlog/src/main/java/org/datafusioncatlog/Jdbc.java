package org.datafusioncatlog;

import java.sql.*;

public class Jdbc {
    protected final String JDBC_DRIVER;
    protected final String DB_URL;
    protected final String USER;
    protected final String PASS;

    protected Connection conn = null;
    protected Statement stmt = null;
    protected DatabaseMetaData metaData = null;
    protected ResultSet rsSchemas = null;
    protected ResultSet rsTables = null;
    protected ResultSet rsColumns = null;
    public Jdbc(String driver, String url, String user, String pass) {
        this.JDBC_DRIVER = driver;
        this.DB_URL=url;
        this.USER=user;
        this.PASS=pass;

    }
    private void  Init() throws Exception{
        Class.forName(JDBC_DRIVER);
    }
    private void Conn() throws SQLException {
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
    }
    protected void GetSchemas() throws SQLException {
        // 获取所有数据库的元数据（MetaData）对象
        metaData = conn.getMetaData();
        // 获取当前数据库中所有表格的名称
        rsSchemas = metaData.getCatalogs();
    }
    public void JdbcInit() throws Exception {
        // 打开连接
        Init();
        System.out.println("连接到数据库...");
        Conn();
    }

}
