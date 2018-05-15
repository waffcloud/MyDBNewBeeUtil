package com.cetc.core.database;

import com.alibaba.fastjson.JSONObject;
import com.cetc.core.util.JdbcUtil;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Description：
 * Created by luolinjie on 2018/3/16.
 */
public class 统计每张表对应的条数 {

    //    target:yanfazu数据库
    private static String db_name = "db_31project";
    private static String url = "jdbc:mysql://localhost:3306/" + db_name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
    private static String username = "root";
    private static String password = "123456";
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static final String default_seperator = ",";

    public static void main(String[] args) throws Exception {

        init();

//        countDBAllTables(db_name);

        closeConn();
    }

    public static void init() {
        try {
            conn = JdbcUtil.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn, pstmt);
    }


    /**
     * 根据入参targetTableArray统计指定表的属性情况
     *
     * @param targetTableArray tablename1,tablename2,tablename3...
     * @param userSeperator    指定分隔符,默认按逗号进行分隔
     * @throws Exception
     */
    public static void countDBAllTables(String targetTableArray, String userSeperator) throws Exception {

        //todo: 1.获取所有表的表名
        String seperator = null;
        if (null == userSeperator) {
            seperator = default_seperator;
        }
        String[] targetTables = targetTableArray.split(seperator);

        //todo: 2.遍历所有表，执行更新操作
        int i = 1;
        int count = 0;
        for (String tb_name : targetTables) {
//            System.out.println("\r\n\r\n正在处理表：---------  " + tb_name + "  ------------");
            int i1 = countTotal(tb_name);
            System.out.println(tb_name + "," + i1);
            count += i1;
        }
        System.out.println("Total:" + count);

    }


    @RequestMapping(value = "/countDBAllTables", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public JSONObject countDBAllTables(String dbName) throws Exception {

        JSONObject result = new JSONObject();

        conn = JdbcUtil.getConnection(url, username, password);


        String SQL =    "SELECT \n" +
                        "table_name,table_rows,table_comment\n" +
                        "FROM \n" +
                        "tables \n" +
                        "WHERE \n" +
                        "TABLE_SCHEMA = '" + dbName + "' \n" +
                        "ORDER BY \n" +
                        "table_rows DESC;";
        PreparedStatement statement = conn.prepareStatement(SQL);
        ResultSet resultSet = statement.executeQuery();
        System.out.println("|--table_name--|--table_rows--|--table_comment--|");
        int i = 1;
        while (resultSet.next()) {
            String table_name = resultSet.getString(1);
            int table_rows = resultSet.getInt(2);
            String table_comment = resultSet.getString(3);
            System.out.println(table_name + "\t" + table_rows + "\t" + table_comment);
            JSONObject cell = new JSONObject();
            cell.put("table_name",table_name);
            cell.put("table_rows",table_rows);
            cell.put("table_comment",table_comment);
            result.put(String.valueOf(i++),cell);
        }
        return result;
    }


    public static int countTotal(String BaseTable) throws Exception {
        String sql = "SELECT  COUNT(1) FROM " + BaseTable;
        pstmt = conn.prepareStatement(sql);
//        System.out.println("preparing sql:" + sql);

        // 5.执行sql语句，得到返回结果
        ResultSet resultSet = pstmt.executeQuery(sql);

        resultSet.next();
        int total = resultSet.getInt(1);
        return total;
    }
}
