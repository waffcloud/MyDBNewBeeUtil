package com.cetc.GIS;


import com.cetc.util.CoordinateTransformer;
import com.cetc.util.JdbcUtil;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Description：批量更新每张表中对应的BaiduMap经纬度字段
 * Created by luolinjie on 2018/2/3.
 */
public class BatchFillCoordinate_Baidu {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static long counter = 1;
    private static long failCounter = 1;
    private static String FilePath = "C:\\Users\\Administrator\\Desktop\\消防隐患数据整理\\地址解析结果-金地工业区.txt";

    public static void main(String[] args) throws IOException {
        Logger logger = Logger.getLogger("BatchFillCoordinate");

        init();
        try {
//            fillCoordinate_Baidu("tb_shops_fire_danger",FilePath);
            fillCoordinate_84("tb_shops_fire_danger", 650);
        } catch (Exception e) {
            System.out.println("error");
            e.printStackTrace();
        }
        closeConn();
    }

    /**
     * 填充经纬度坐标信息 -- 根据 商铺名称+楼栋 定位填充
     *
     * @param table_name
     * @throws SQLException
     * @throws InterruptedException
     */
    public static void fillCoordinate_Baidu(String table_name, String filePath) throws SQLException, IOException {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String s = null;
            reader.readLine();
            while ((s = reader.readLine()) != null) {
                String[] split = s.split(" ");
                if (split.length == 3) {

                    //1.楼栋号
                    String buildNum = split[1];
                    System.out.println(split[0] + ">>" + buildNum);

                    //2.商铺名称--经纬度
                    String organizationName_corrdinate = split[2];

                    String[] split1 = organizationName_corrdinate.split("\\:");

                    //商铺名称
                    String address = split1[0];

                    String corrdinate = split1[1];
                    String[] longitude_latitude = corrdinate.split("\\,");

                    //经度+纬度
                    String longitude = longitude_latitude[0];
                    String latitude = longitude_latitude[1];


                    String sql = "UPDATE " + table_name + " SET longitude='" + longitude + "',latitude='" + latitude + "' WHERE organization_name='" + address + "' AND address='" + buildNum + "'";
                    PreparedStatement pstmt = conn.prepareStatement(sql);
                    int count = pstmt.executeUpdate();
                    if (count > 0) {
                        System.out.println("SUCCESS!," + address + " >> " + longitude + "--" + latitude + "--\n\r更新总记录数" + counter++);
                    }
                } else if (split.length == 4) {

                    //1.楼栋号
                    String buildNum = split[1] + split[2];
                    System.out.println(split[0] + ">>" + buildNum);

                    //2.商铺名称--经纬度
                    String organizationName_corrdinate = split[3];

                    String[] split1 = organizationName_corrdinate.split("\\:");

                    //商铺名称
                    String address = split1[0];

                    String corrdinate = split1[1];
                    String[] longitude_latitude = corrdinate.split("\\,");

                    //经度+纬度
                    String longitude = longitude_latitude[0];
                    String latitude = longitude_latitude[1];


                    String sql = "UPDATE " + table_name + " SET longitude='" + longitude + "',latitude='" + latitude + "' WHERE organization_name='" + address + "' AND address='" + buildNum + "'";
                    PreparedStatement pstmt = conn.prepareStatement(sql);
                    int count = pstmt.executeUpdate();
                    if (count > 0) {
                        System.out.println("SUCCESS!," + address + " >> " + longitude + "--" + latitude + "--\n\r更新总记录数" + counter++);
                    }
                } else {
                    System.out.println("ERROR!! invalid data length:" + split.length);
                }
            }


        } catch (FileNotFoundException e) {
            System.out.println("读取文件：" + filePath + "失败！");
            e.printStackTrace();
        }

    }

    public static void fillCoordinate_84(String table_name, int tableSize) throws SQLException {
        try {

            String longitude84 = null;
            String latitude84 = null;
            for (int i = 1; i <= tableSize; i++) {

                String querySQL = "SELECT longitude,latitude FROM " + table_name + " WHERE id=" + i;
                PreparedStatement preparedStatement = conn.prepareStatement(querySQL);
                ResultSet resultSet = preparedStatement.executeQuery();
                String longitude = null;
                String latitude = null;
                while (resultSet.next()) {
                    longitude = resultSet.getString(1);
                    latitude = resultSet.getString(2);
                }
                //解析经纬度坐标  baidu --> 84
                double[] trans_res = CoordinateTransformer.bd09_to_wgs84(Double.parseDouble(longitude), Double.parseDouble(latitude));
                longitude84 = String.valueOf(trans_res[0]);
                latitude84 = String.valueOf(trans_res[1]);

                String sql = "UPDATE " + table_name + " SET longitude_84='" + longitude84 + "',latitude_84='" + latitude84 + "' WHERE id=" + i;
                PreparedStatement pstmt = conn.prepareStatement(sql);
                int count = pstmt.executeUpdate();
                if (count > 0) {
                    System.out.println("SUCCESS!," + i + " >> " + longitude84 + "--" + latitude84 + "--\n\r更新总记录数" + counter++);
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
            conn = JdbcUtil.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    //    @After
    public static void closeConn() {
        JdbcUtil.close(conn, pstmt);
    }

    public void test() {
        String name = " 南山实验 教育集  团鼎   太小学 ";
        String s = name.replaceAll(" ", "");
        System.out.println(s);
    }

}
