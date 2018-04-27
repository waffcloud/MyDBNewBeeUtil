package com.cetc.database;

import com.cetc.util.DBUtil;
import com.cetc.util.JdbcUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Description：
 * Created by luolinjie on 2018/4/24.
 */
public class 批量执行纯insertSQL文件 {

    //配置文件，罗列了所有要执行的SQL文件，每个SQL文件中是insert语句
    public static final String folder_name_path = "C:\\Users\\Administrator\\Desktop\\31项目数据建模\\消防隐患数据整理\\base_data";

    //    source数据库
    private static String IP = "192.168.16.103";
    private static String DB_name_source = "gengxin";
    private static String user_source = "root";
    private static String password_source = "pingtaizu972";

    private static String url1 = "jdbc:mysql://" + IP + ":3306/" + DB_name_source + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";


    //    target数据库
    private static String IP_target = "localhost";
    private static String DB_name_target = "31project_april";
    private static String user_target = "root";
    private static String password_target = "123456";

    private static String url2 = "jdbc:mysql://" + IP_target + ":3306/" + DB_name_target + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    private static Connection conn_source = null;
    private static Connection conn_target = null;
    private static PreparedStatement pstmt = null;
    private static int SuccessCounter = 0;
    private static int FailCounter = 0;
    private static HashSet<String> targetTableSet = new HashSet<String>();
    private static HashSet<String> sourceTableSet = new HashSet<String>();


    public static void main(String[] args) throws IOException, SQLException {
        init();
        //获取源数据库和目标数据库中所有表名（全部转换成小写）
        //获取源数据库和目标数据库中所有表名（全部转换成小写）
        DBUtil.getDBAllTableNames(targetTableSet, DB_name_target, conn_target);
        DBUtil.getDBAllTableNames(sourceTableSet, DB_name_source, conn_source);

        ExecuteSQLFiles(folder_name_path);

        closeConn();
    }


    /***
     * 只执行SQL的insert操作，前提是表结构必须建立好
     *
     * @param folder_name
     * @throws SQLException
     * @throws IOException
     */
    public static void ExecuteSQLFiles(String folder_name) throws SQLException, IOException {
        //todo:1.读取文件夹下的sql文件
        File file = new File(folder_name);
        File[] files = file.listFiles();
        int coun = 0;
        for (File f : files) {
            String fName = f.getName();
            CreateOrTruncateTargetTable(fName.toLowerCase().split("\\.")[0]);
            coun++;
        }
        System.out.println("\r\n################                         ###############");
        System.out.println("                读取完成，共有sql文件:" + coun + "个");
        System.out.println("################                         ###############");


        //todo:2.执行sql
        //汇总报告
        HashSet<String> reports = new HashSet<String>();
        int fcounter = 1;
        for (File f : files) {
            int singleFileSqlCounter_success = 0;
            int singleFileSqlCounter_fail = 0;
            BufferedReader br = new BufferedReader(new FileReader(f));
            System.out.println("\r\n\r\n******正在处理第"+fcounter+++"/"+files.length+"个文件:"+f.getName());
            while (br.ready()) {
                String sqlString = br.readLine();
                if (null == sqlString || "".equals(sqlString)) {
                    break;
                }
                PreparedStatement pstmt = conn_target.prepareStatement(sqlString);
                int i = pstmt.executeUpdate();
                if (i > 0) {
                    singleFileSqlCounter_success++;
                    SuccessCounter++;
                } else {
                    singleFileSqlCounter_fail++;
                    FailCounter++;
                }
            }
            //todo:3.打印计数
            reports.add("\r\n"+f.getName()+"执行SQL成功数：" + singleFileSqlCounter_success + "\t-----失败数： " + singleFileSqlCounter_fail);
            System.out.println("\n" + f.getName() + "：  \r\n执行SQL成功数：" + singleFileSqlCounter_success + "-----失败数： " + singleFileSqlCounter_success);

        }
        System.out.println(" \r\n################                         ###############");
        System.out.println(" \r\n汇总：执行SQL成功数：" + SuccessCounter + "-----失败数： " + FailCounter);
        System.out.println(" ################                         ###############");
        System.out.println("\r\n具体结果："+reports.toString());

    }


    /**
     * 执行insert之前先检查是否存在这张表，没有的话从source数据库拿到DDL进行建表，有的话对其执行truncate清空操作
     *
     * @param tableName
     * @throws SQLException
     * @throws IOException
     */
    public static void CreateOrTruncateTargetTable(String tableName) throws SQLException, IOException {
        String DDL = null;
        //todo:1.检查tagetDB是否存在这张表
        if (!targetTableSet.contains(tableName)) {//todo:2.没有的话从source数据库拿到DDL进行建表
            //todo:2.source数据库中表名可能为大写，因此执行时候需要判断是大写还是小写
            boolean isUpperCase = false;
            if (sourceTableSet.contains(tableName) || (isUpperCase = sourceTableSet.contains(tableName.toUpperCase()))) {
                String sql = null;
                if (isUpperCase) {
                    sql = "show create table " + tableName.toUpperCase();
                } else {
                    sql = "show create table " + tableName;
                }

                PreparedStatement pstmt = conn_source.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    DDL = rs.getString(2);
                }
            }

            //建表
            PreparedStatement ps = conn_target.prepareStatement(DDL);
            ps.execute();
            System.out.println("\r\nsuccess！操作：在目标数据库创建表：" + tableName);

        } else {  //todo:3.有的话对其执行truncate清空操作
            String DoTruncate = "truncate TABLE " + tableName;
            PreparedStatement ps = conn_target.prepareStatement(DoTruncate);
            ps.execute();
            System.out.println("\r\nsuccess！操作：清空表：" + tableName);
        }
    }

    /***
     * 使用source命令加载sql文件
     *
     * @param folder_name
     * @throws SQLException
     * @throws IOException
     */
    public static void SourceSQLFiles(String folder_name) throws SQLException, IOException {

    }


    public static void init() {
        try {
            conn_source = JdbcUtil.getConnection(url1, user_source, password_source);
            conn_target = JdbcUtil.getConnection(url2, user_target, password_target);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn_source, pstmt);
        JdbcUtil.close(conn_target, pstmt);
    }
}
