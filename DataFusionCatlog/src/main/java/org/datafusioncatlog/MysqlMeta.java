package org.datafusioncatlog;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlMeta extends  Jdbc {

    private List<String> command = new ArrayList<>();
    private  List<String> cmmandView = new ArrayList<>();
    public MysqlMeta(String driver, String url, String user, String pass) {
        super(driver, url, user, pass);
    }

    public List<String> getCommand() {
        return command;
    }

    public List<String> getCmmandView() {
        return cmmandView;
    }
    public void  SaveCatlogInfo() throws SQLException{
        GetSchemas();
        try {
            // 注册 JDBC 驱动程序
            while (rsSchemas.next()) {
                String schema= rsSchemas.getString("TABLE_CAT");
                System.out.println("SCHEMA: "+ schema);
                rsTables = metaData.getTables(schema,null,null,new String[]{"TABLE","VIEW"});
                while (rsTables.next()) {
                    String tableName = rsTables.getString("TABLE_NAME");
                    String tableType = rsTables.getString("TABLE_TYPE");
                    System.out.println("===================");
                    System.out.println("Table name: " + tableName);

                    String sqlv = "insert into schema_tables values('"+schema+"','"+tableName+"','"+tableType+"');";
                    cmmandView.add(sqlv);

                    // 获取当前表格的字段信息
                    rsColumns = metaData.getColumns(schema, null, tableName, "%");
                    while (rsColumns.next()) {
                        String columnName = rsColumns.getString("COLUMN_NAME");
                        String columnType = rsColumns.getString("TYPE_NAME");
                        int columnSize = rsColumns.getInt("COLUMN_SIZE");
                        boolean isNullable = (rsColumns.getString("IS_NULLABLE").equals("YES"));

                        // 输出字段信息
                        System.out.println("\tColumn name: " + columnName);
                        System.out.println("\tColumn type: " + columnType);
                        System.out.println("\tColumn size: " + columnSize);
                        System.out.println("\tCan be null: " + isNullable);
                        String sql = "insert into public.cloumns values('"+tableName+"','"+columnName+"','"+columnType+"',"+columnSize+","+isNullable+");";
                        System.out.println("sql: "+sql);
                        command.add(sql);

                    }
                }
            }

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException s) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
            try {
                if (rsTables != null)
                    rsTables.close();
            } catch (SQLException se1) {
                se1.printStackTrace();
            }
            try {
                if (rsColumns != null)
                    rsColumns.close();
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }


}
