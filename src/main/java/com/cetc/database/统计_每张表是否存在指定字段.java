package com.cetc.database;

import com.cetc.util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Description：
 * Created by luolinjie on 2018/4/26.
 */
public class 统计_每张表是否存在指定字段 {


    //    target:yanfazu数据库
    private static String IP = "192.168.16.103";
    private static String DB_Name = "db_31project";
    private static String user_source = "root";
    private static String password_source = "123456";
    private static String url1 = "jdbc:mysql://" + IP + ":3306/" + DB_Name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    private static Connection conn = null;
    private static PreparedStatement pstmt = null;


    private static String[] targetColumnName = {"jd84", "wd84"};

    public static void main(String[] args) throws Exception {

        init();

        summarize(DB_Name, targetColumnName);

        closeConn();
    }

    public static void init() {
        try {
            conn = JdbcUtil.getConnection(url1, user_source, password_source);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn, pstmt);
    }

    public static void summarize(String DBName, String[] targetColumnName) throws SQLException {

        //todo:1.获取库中所有的表组成的列表
        String SQL_exportAllTbNames = "select table_name from  tables where TABLE_SCHEMA = '" + DBName + "' ";

        PreparedStatement ps = conn.prepareStatement(SQL_exportAllTbNames);
        ResultSet rs1 = ps.executeQuery();
        HashSet<String> tbNameSet = new HashSet<String>();

        while (rs1.next()) {
            String tableName = rs1.getString(1);
            tbNameSet.add(tableName);
        }
        Iterator<String> itr = tbNameSet.iterator();
        while (itr.hasNext()) {//todo:2.遍历，判断该表中是否存在指定字段
            String singleTableName = itr.next();

            String SQL_getAllColumnName = "SELECT COLUMN_NAME  FROM information_schema.columns " +
                    "WHERE table_schema = '" + DBName + "' " +
                    "AND table_name = '" + singleTableName + "'";
            HashSet<String> columnNameSet = new HashSet<String>();
            PreparedStatement ps2 = conn.prepareStatement(SQL_getAllColumnName);
            ResultSet rs2 = ps2.executeQuery();
            while (rs2.next()) {
                String columnName = rs2.getString(1);
                columnNameSet.add(columnName);
            }



        }


    }


}
