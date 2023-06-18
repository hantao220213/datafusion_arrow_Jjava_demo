package org.datafusioncatlog;

import java.util.List;

public class Main {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://192.168.174.138:3306";
    static final String USER = "root";
    static final String PASS = "123456";

    static final String pgJDBC_DRIVER = "org.postgresql.Driver";
    static final String pgDB_URL = "jdbc:postgresql://192.168.174.138/postgres";
    static final String pgUSER = "postgres";
    static final String pgPASS = "123456";

    public static void main(String[] args) throws Exception {
        MysqlMeta jdbc = new MysqlMeta(JDBC_DRIVER,DB_URL,USER,PASS);
        jdbc.JdbcInit();
        jdbc.SaveCatlogInfo();
        List<String> command = jdbc.getCommand();
        List<String> commandView = jdbc.getCmmandView();
        System.out.println("SQL: "+command.toString());


        PostgresJdbc postgresJdbc = new PostgresJdbc(pgJDBC_DRIVER,pgDB_URL,pgUSER,pgPASS);

        postgresJdbc.JdbcInit();
        for (String sql:command)
        {
            System.out.println("insert table sql : " + sql);
            postgresJdbc.ExcuteCommand(sql);
        }

        for (String sql: commandView){
            System.out.println("insert schema sql : " + sql);
            postgresJdbc.ExcuteCommand(sql);
        }

        System.out.println("Hello world!");
    }
}