package com.cetc.GIS;


import com.cetc.util.CoordinateTransformer;
import com.cetc.util.GaodeMapUtil;
import com.cetc.util.JdbcUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Description：通过高德在线接口，批量更新每张表中对应的高德aMap经纬度字段
 * Created by luolinjie on 2018/4/18
 */
public class BatchFillCoordinate_Gaode {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static long counter = 1;

    private static String IP = "192.168.16.220";//研发组互联网测试机
//    private static String IP = "localhost";
    private static String DB_name = "31project_april";
    private static String username = "root";
    private static String password = "123456";
    private static String url = "jdbc:mysql://"+IP+":3306/"+DB_name+"?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    private static long failCounter = 1;
    private static String FilePath = "C:\\Users\\Administrator\\Desktop\\消防隐患数据整理\\地址解析结果-金地工业区.txt";

    public static void main(String[] args) throws IOException {
        Logger logger = Logger.getLogger("BatchFillCoordinate_Gaode");

        init();
        try {
            fillCoordinate_84("xiaohuoshuan_001", 3);
        } catch (Exception e) {
            System.out.println("error");
            e.printStackTrace();
        }
        closeConn();
    }

    public static void fillCoordinate_84(String table_name, int tableSize) throws SQLException {
        try {

            String longitude84 = null;
            String latitude84 = null;
            String jd = null;
            String wd = null;
            for (int i = 1; i <= tableSize; i++) {
                //todo:1.查询：商铺楼栋+商铺名称（address+organization_name）
                String querySQL = "SELECT address FROM " + table_name + " WHERE id=" + i;
                PreparedStatement preparedStatement = conn.prepareStatement(querySQL);
                ResultSet resultSet = preparedStatement.executeQuery();

                String address = null;
                String organization_name = null;
                while (resultSet.next()) {
                    address = resultSet.getString(1);
                }

                String param_address = "沙头街道 " + address + " " ;

                //todo:2.根据 商铺楼栋+商铺名称 获取高德经纬度
                String coordinates_gaode = GaodeMapUtil.getOnlineCoordinates(param_address, "深圳").getJSONObject("data").getJSONArray("geocodes").getJSONObject(0).getString("location");

                String[] lng_lat = coordinates_gaode.split(",");
                if (lng_lat.length != 2) {
                    System.out.println("解析地址：获取高德经纬度失败！ 当前id：" + i);
                    continue;
                }

                //获取到经纬度
                jd = lng_lat[0];
                wd = lng_lat[1];

                //todo:3.解析经纬度坐标  gaode --> 84
                double[] trans_res = CoordinateTransformer.gcj02_to_wgs84(Double.parseDouble(jd), Double.parseDouble(wd));
                longitude84 = String.valueOf(trans_res[0]);
                latitude84 = String.valueOf(trans_res[1]);

                //todo:4.更新 高德、84 两类坐标
                String sql = "UPDATE " + table_name + " SET jd='" + jd + "',wd='" + wd + "' ,jd84='" + longitude84 + "',wd84='" + latitude84 + "' WHERE id=" + i;
                PreparedStatement pstmt = conn.prepareStatement(sql);
                int count = pstmt.executeUpdate();
                if (count > 0) {
                    System.out.println("SUCCESS! " + i + " >> "+param_address+" >>[ " + longitude84 + "," + latitude84 + " ]--\n\r更新总记录数" + counter++);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    //    @Before
    public static void init() {
        try {
            conn = JdbcUtil.getConnection(url,username,password);
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
