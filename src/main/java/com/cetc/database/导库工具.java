package com.cetc.database;

import com.cetc.util.JdbcUtil;
import com.cetc.util.UuIdGeneratorUtil;
import org.dom4j.DocumentException;

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

/**
 * Description：导库工具
 *
 *  注意：需要先在target端建好表,表结构关键字段一定要一致
 *
 * Created by luolinjie on 2018/3/16.
 */
public class 导库工具 {

    //    source:pingtaizu数据库
    private static String IP = "192.168.16.103";
    private static String DB_name = "gengxin";
    private static String user_source = "root";
    private static String password_source = "pingtaizu972";

    private static String url1 = "jdbc:mysql://"+IP+":3306/"+DB_name+"?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    //    target:yanfazu数据库
    private static String IP_target = "192.168.16.54";
    private static String DB_name_target = "31project_april";
    private static String user_target = "root";
    private static String password_target = "123456";

    private static String url2 = "jdbc:mysql://"+IP_target+":3306/"+DB_name_target+"?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    private static Connection conn_source = null;
    private static Connection conn_target = null;
    private static PreparedStatement pstmt = null;
    /**
     * 文件路径
     **/
    private static String DataBackupPath = "table_list.txt";

    public static void main(String[] args) throws DocumentException {
        init();
        try {
            URL url = 导库工具.class.getClassLoader().getResource(DataBackupPath);
            BufferedReader reader = new BufferedReader(new FileReader(new File(url.getFile())));

            String tb_name;
            while (true) {
                tb_name = reader.readLine();
                if ( null == tb_name) break; //读到了文件末尾，结束
                if ("".equals(tb_name)  || tb_name.startsWith("#")) {
                    continue;
                }
                A_Test(tb_name);
            }

        } catch (Exception e) {
            System.out.println("error in main");
            e.printStackTrace();
        }
        closeConn();
    }

    public static void init() {
        try {
            conn_source = JdbcUtil.getConnection(url1,user_source,password_source);
            conn_target = JdbcUtil.getConnection(url2,user_target,password_target);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn_source, pstmt);
        JdbcUtil.close(conn_target, pstmt);
    }


    /***
     * 将A库中的数据导入B库，前提是必须在B库中提前创建好一样的表结构
     * @param tb_name
     * @throws SQLException
     * @throws IOException
     * @throws DocumentException
     */
    public static void A_Test(String tb_name) throws SQLException, IOException, DocumentException {

        String sql0 = "TRUNCATE " + tb_name;

        PreparedStatement pstmt0 = conn_source.prepareStatement(sql0);
        boolean i0 = pstmt0.execute();
        System.out.println("##########################################################################################");
        System.out.println("       ########                         #######");
        System.out.println("                成功清空! table_name：" + tb_name);
        System.out.println("       ########                         #######");
        System.out.println("##########################################################################################");


        int SuccessCounter = 0;
        int FailCounter = 0;

        String sql_colums = "";
        String values = "";

        //源：平台组数据库
        String sql1 = "SELECT COLUMN_NAME from information_schema.columns where table_name= '"+tb_name+"'";
        pstmt = conn_source.prepareStatement(sql1);
        System.out.println(sql1);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()){
            String column_name = rs.getString(1);
            System.out.println(column_name);
        }

        String sql = "insert into " + tb_name + " (uuid," + sql_colums + ",create_time)"
                + "values('"
                + UuIdGeneratorUtil.getCetcCloudUuid("31_project") + "','"
                + values + "','"
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime())
                + "')";

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



}



