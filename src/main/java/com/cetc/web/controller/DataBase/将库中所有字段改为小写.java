package com.cetc.web.controller.DataBase;

import com.cetc.core.util.DBUtil;
import com.cetc.core.util.JdbcUtil;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Description：
 * Created by luolinjie on 2018/4/27.
 */
public class 将库中所有字段改为小写 {
    private final static Logger logger = LoggerFactory.getLogger(将库中所有字段改为小写.class);

    /************************  配置信息 ******************************************/
    /**
     * source:源数据库
     **/
/*    private static String IP_source = "localhost";
    private static String DB_name_source = "test";
    private static String user_source = "root";
    private static String password_source = "123456";*/
    private static String IP_source = "192.168.16.195";
    private static String DB_name_source = "31project_april";
    private static String user_source = "root";
    private static String password_source = "123456";


    private static String url_source = "jdbc:mysql://" + IP_source + ":3306/" + DB_name_source + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    /****************************************************************************/

    private static Connection conn_source = null;
    private static PreparedStatement pstmt = null;

    private static int SuccessCounter = 0;
    private static int FailCounter = 0;


    public static void main(String[] args) throws DocumentException, SQLException {
        init();

//        parseDBAllColumnNameToLowerCase();
        parseTableSetAllColumnNameToLowerCase();

        closeConn();
    }


    public static void parseDBAllColumnNameToLowerCase() throws SQLException {
        //todo:1 获取当前数据库的所有表名组成的列表
        Set set = DBUtil.getDBAllTableNames(DB_name_source, conn_source);
        Iterator iterator = set.iterator();

        while (iterator.hasNext()) {
            String tableName = (String) iterator.next();
            //todo:2 获取当前表所有字段名组成的列表
            String SQL_getAllColumns = "SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT FROM `information_schema`.`columns` " +
                    "WHERE `table_schema` = '" + DB_name_source + "'" +
                    "AND `table_name` = '" + tableName + "'";
            PreparedStatement ps1 = conn_source.prepareStatement(SQL_getAllColumns);
            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()){
                //todo:3 获取字段,并获取其字段类型
                String columnName = rs1.getString(1);
                String columnType = rs1.getString(2);
                String columnComment = rs1.getString(3);

                System.out.println("colunmName --> "+columnName);
                //如果字段名称已经是小写的话，就不做修改
                if (columnName.equals(columnName.toLowerCase())){
                    continue;
                }
                //alter table 表名  change 旧字段名 新字段名 新数据类型
                String sql2 =  "ALTER TABLE " + tableName + " CHANGE `"+columnName+"` `"
                        +columnName.toLowerCase()+"` "+columnType+" COMMENT '"+columnComment+"'";
                PreparedStatement ps2 = conn_source.prepareStatement(sql2);
                boolean execute = ps2.execute();
                System.out.println(sql2);
                System.out.println("成功！"+SuccessCounter++);

            }
        }

    }
    public static void parseTableSetAllColumnNameToLowerCase() throws SQLException {
        //todo:1 获取当前数据库的所有表名组成的列表
        Set set = new HashSet();

        String[] tableArray = {
                "anquanbiaozhunhuaqiye_001",
                "weihuapinqiye_001",
                "weihuo_renyuan_001"
        };

        for (int i=0;i<tableArray.length;i++){
            set.add(tableArray[i]);
        }
        Iterator iterator = set.iterator();

        while (iterator.hasNext()) {
            String tableName = (String) iterator.next();
            //todo:2 获取当前表所有字段名组成的列表
            String SQL_getAllColumns = "SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT FROM `information_schema`.`columns` " +
                    "WHERE `table_schema` = '" + DB_name_source + "'" +
                    "AND `table_name` = '" + tableName + "'";
            PreparedStatement ps1 = conn_source.prepareStatement(SQL_getAllColumns);
            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()){
                //todo:3 获取字段,并获取其字段类型
                String columnName = rs1.getString(1);
                String columnType = rs1.getString(2);
                String columnComment = rs1.getString(3);

                System.out.println("colunmName --> "+columnName);
                //如果字段名称已经是小写的话，就不做修改
                if (columnName.equals(columnName.toLowerCase())){
                    continue;
                }
                //alter table 表名  change 旧字段名 新字段名 新数据类型
                String sql2 =  "ALTER TABLE " + tableName + " CHANGE `"+columnName+"` `"
                        +columnName.toLowerCase()+"` "+columnType+" COMMENT '"+columnComment+"'";
                PreparedStatement ps2 = conn_source.prepareStatement(sql2);
                boolean execute = ps2.execute();
                System.out.println(sql2);
                System.out.println("成功！"+SuccessCounter++);

            }
        }

    }
    public static void init() {
        try {
            conn_source = JdbcUtil.getConnection(url_source, user_source, password_source);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public static void closeConn() {
        JdbcUtil.close(conn_source, pstmt);
    }


}