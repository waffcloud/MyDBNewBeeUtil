package com.cetc.GIS;


import com.cetc.util.CoordinateTransformer;
import com.cetc.util.JdbcUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Description：
 * Created by luolinjie on 2018/4/18
 */
public class 根据百度经纬度解析出84坐标并填充 {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static long counter = 1;

    private static String IP = "192.168.16.220";//研发组互联网测试机
    //    private static String IP = "localhost";
    private static String DB_name = "31project_april";
    private static String username = "root";
    private static String password = "123456";
    private static String url = "jdbc:mysql://" + IP + ":3306/" + DB_name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    private static long successCounter = 0;
    private static long failCounter = 0;

    public static void main(String[] args) throws IOException {
        Logger logger = Logger.getLogger("根据百度经纬度解析出84坐标并填充");

        init();
        try {
            TransmitBaiduTo84("xiaohuoshuan_001");
        } catch (Exception e) {
            System.out.println("error");
            e.printStackTrace();
        }
        closeConn();
    }

    public static void TransmitBaiduTo84(String table_name) throws SQLException {
        try {

            String longitude84 = null;
            String latitude84 = null;
            String id = null;
            String jd = null;
            String wd = null;
            //todo：1.获取表中 jd、wd 数据
            String querySQL = "SELECT id,jd,wd FROM " + table_name;
            PreparedStatement preparedStatement = conn.prepareStatement(querySQL);
            ResultSet resultSet = preparedStatement.executeQuery();

            String address = null;
            String organization_name = null;
            while (resultSet.next()) {
                id = resultSet.getString(1);
                jd = resultSet.getString(2);
                wd = resultSet.getString(3);

                //todo:1.转换经纬度坐标  baidu --> 84
                double[] trans_res = CoordinateTransformer.bd09_to_wgs84(Double.parseDouble(jd), Double.parseDouble(wd));
                longitude84 = String.valueOf(trans_res[0]);
                latitude84 = String.valueOf(trans_res[1]);

                //todo:4.更新 高德、84 两类坐标
                String sql = "UPDATE " + table_name + " SET jd='" + jd + "',wd='" + wd + "' ,jd84='" + longitude84 + "',wd84='" + latitude84 + "' WHERE id=" + id;
                PreparedStatement pstmt = conn.prepareStatement(sql);
                int count = pstmt.executeUpdate();
                if (count > 0) {
                    System.out.println("SUCCESS! " + id + " >> [ " + jd + "," + wd + " ]" +
                            ">>" + longitude84 + "," + latitude84 + " --\n\r更新总记录数" + counter++);
                }

            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }


    //    @Before
    public static void init() {
        try {
            conn = JdbcUtil.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    //    @After
    public static void closeConn() {
        JdbcUtil.close(conn, pstmt);
    }


}
