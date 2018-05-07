package com.cetc.database;

import com.cetc.util.DBUtil;
import com.cetc.util.JdbcUtil;
import com.cetc.util.UuIdGeneratorUtil;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

/**
 * Description：source_target两库对导工具
 * <p>
 * 注意：需要先在target端建好表,表结构关键字段一定要一致
 * <p>
 * Created by luolinjie on 2018/3/16.
 */
public class source_target两库对导工具 {

    /************************  配置信息 ******************************************/
    /**
     * source:源数据库
     **/
    private static String IP_source = "192.168.16.103";
    private static String DB_name_source = "gengxin";
    private static String user_source = "root";
    private static String password_source = "pingtaizu972";


    /**
     * target:yanfazu数据库
     **/
    private static String IP_target = "192.168.16.54";
    private static String DB_name_target = "31project_april";
    private static String user_target = "root";
    private static String password_target = "123456";

    private static String url1 = "jdbc:mysql://" + IP_source + ":3306/" + DB_name_source + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
    private static String url2 = "jdbc:mysql://" + IP_target + ":3306/" + DB_name_target + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
    /****************************************************************************/

    private static Connection conn_source = null;
    private static Connection conn_target = null;
    private static PreparedStatement pstmt = null;

    private static int SuccessCounter = 0;
    private static int FailCounter = 0;
    private static Set<String> targetTableSet ;
    private static Set<String> sourceTableSet ;

    private static final Logger logger = LoggerFactory.getLogger(source_target两库对导工具.class);

    /**
     * 文件路径
     **/
    private static String ConfigFilePath = "***.txt";

    public static void main(String[] args) throws DocumentException, SQLException {
        init();

        //获取源数据库和目标数据库中所有表名（全部转换成小写）
        targetTableSet = DBUtil.getDBAllTableNames( DB_name_target, conn_target);
        sourceTableSet = DBUtil.getDBAllTableNames( DB_name_source, conn_source);

        try {
            URL url = source_target两库对导工具.class.getClassLoader().getResource(ConfigFilePath);
            BufferedReader reader = new BufferedReader(new FileReader(new File(url.getFile())));

            String tb_name = null;
            while (true) {
                tb_name = reader.readLine();
                if (null == tb_name) break; //读到了文件末尾，结束
                if ("".equals(tb_name) || tb_name.startsWith("#")) {
                    continue;
                }
                DoTransmit(tb_name);
            }

        } catch (Exception e) {
            System.out.println("error in main");
            e.printStackTrace();
        }
        closeConn();
    }


    /***
     * 将A库中的表导入B库，B库不存在指定表时将自动创建
     *
     * @param tableName
     * @throws SQLException
     * @throws IOException
     * @throws DocumentException
     */
    public static void DoTransmit(String tableName) throws SQLException, IOException, DocumentException {
        String DDL = null;
        //todo:1.target数据库中是否存在这张表，如果有则直接拷贝，如果没有则从source库中创建后进行拷贝  如果source没有这张表则记录后跳过
        if (targetTableSet.contains(tableName.toLowerCase())) {
            startCopy(tableName,true);
        } else {
            boolean isUpperCase = false;
            //todo:从源数据库中拿到DDL
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

                // todo:创建表
                PreparedStatement ps = conn_target.prepareStatement(DDL);
                ps.execute();
                System.out.println("\r\nsuccess！操作：在目标数据库创建表：" + tableName);

                startCopy(tableName, false);
            }
        }
    }

    public static void startCopy(String tableName, boolean doTruncate) throws SQLException {

        //todo:1.根据传入条件决定是否清空表
        if (doTruncate) {
            String SQL_Truncate = "TRUNCATE " + tableName;

            PreparedStatement pstmt0 = conn_source.prepareStatement(SQL_Truncate);
            pstmt0.execute();
            System.out.println("##########################################################################################");
            System.out.println("       ########                         #######");
            System.out.println("                成功清空! table_name：" + tableName);
            System.out.println("       ########                         #######");
            System.out.println("##########################################################################################");
        }

        //2.查询源库并insert至target库
        int SuccessCounter = 0;
        int FailCounter = 0;

        String sql_colums = "";

        //查询源数据库，取出结果集
        String SQL_querySource = "SELECT * from " + tableName;
        pstmt = conn_source.prepareStatement(SQL_querySource);
        logger.info(SQL_querySource);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            String column_name = rs.getString(1);
            logger.info(column_name);
        }

        String sql = "";
//        String sql = "insert into " + tableName
//                + "values('"
//                + UuIdGeneratorUtil.getCetcCloudUuid("31_project") + "','"
//                + values + "','"
//                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime())
//                + "')";

        pstmt = conn_target.prepareStatement(sql);
        System.out.println(sql);
        int res_count = pstmt.executeUpdate();
        if (res_count > 0) {
            System.out.println("成功!" + ++SuccessCounter);
        } else {
            System.out.println("失败!" + ++FailCounter);
        }
        System.out.println("\n成功数：" + SuccessCounter + "-----失败数： " + FailCounter);
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



