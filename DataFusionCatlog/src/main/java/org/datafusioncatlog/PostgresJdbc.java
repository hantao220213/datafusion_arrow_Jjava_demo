package org.datafusioncatlog;
import java.sql.*;
public class PostgresJdbc extends Jdbc {

    public PostgresJdbc(String driver, String url, String user, String pass) {
        super(driver, url, user, pass);
    }

    public void  ExcuteCommand(String sql) throws Exception{
        Statement stmt = conn.createStatement();
        Boolean rs = stmt.execute(sql);
    }



}
