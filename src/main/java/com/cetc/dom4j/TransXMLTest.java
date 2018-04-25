package com.cetc.dom4j;

import com.cetc.util.JdbcUtil;
import com.cetc.util.UuIdGeneratorUtil;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Description：
 * Created by luolinjie on 2018/3/7.
 */
public class TransXMLTest {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    /**
     * 爬取数据备份文件路径
     **/
    private static String DataBackupPath = "C:\\Users\\Administrator\\Desktop\\31接入系统数据梳理\\爬取数据备份\\";
    /**
     * 标签映射文件规则
     */
    private static String tagMappingPath = DataBackupPath + "标签映射规则\\";

    public static void main(String[] args) throws DocumentException {
        init();
        try {
            /** INSERT： 方法名_表的编号(数据库表名,爬取数据的文件夹名,root_grandpa标签名,root_grandpa_father标签名) **/
            //67 待办结案件
//            A_Test("daibanjieaj_001", "daibanjie_1", "banjie", "item");
//            B_Test("daibanjieaj_001", "daibanjie_2", "list", "item", "ajbh");

            //64 待受理案件
//            A_Test("daishouliaj_001", "daishouli_旧的\\daishouli_1", "tb", "item");
//            B_Test("daishouliaj_001", "daishouli_旧的\\daishouli_2", "list", "item", "lsh");

            //65 待派遣案件
//            A_Test("daipaiqianaj_001","daipaiqian_1","banjie", "item");
//            B_Test("daipaiqianaj_001","daipaiqian_2", "list", "item", "ajbh");

            // 70.延时核准案件
//            A_Test("yanshihezhun_001","yshz1","banjie", "item");
//            B_Test("yanshihezhun_001","yshz_2", "list", "item", "ajbh");

            // 68.处理中案件
//            A_Test("chulizhongaj_001", "daichuli_1","banjie", "item");
//            B_Test("chulizhongaj_001", "daichuli_2", "list", "item", "ajbh");


            // 66.退回案件
//            A_Test("tuihuiaj_001", "tuihuianjian_1", "banjie", "item");
//            B_Test("tuihuiaj_001", "tuihuishijian_2", "list", "item", "ajbh");

            // 69.已办结案件
//            A_Test("yibanjieaj_001", "yibanjie_1", "banjie", "item");
//            B_Test("yibanjieaj_001", "yibanjie_2", "list", "item", "ajbh");

            // 77.车辆上线统计
            A_Test("weihuo_shangxiantj_001", "车辆上线统计_2", "tb", "item");

        } catch (Exception e) {
            System.out.println("error in main");
            e.printStackTrace();
        }
        closeConn();
    }

    public static void init() {
        try {
            conn = JdbcUtil.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn, pstmt);
    }


    /**
     * 64.案件--列表内容
     *
     * @throws DocumentException
     * @throws SQLException
     */
    public static void A_Test(String tb_name, String folder_name, String GrandPaTagName, String FatherTagName) throws SQLException, IOException, DocumentException {

        String sql0 = "TRUNCATE " + tb_name;

        PreparedStatement pstmt0 = conn.prepareStatement(sql0);
        boolean i0 = pstmt0.execute();
        System.out.println("##########################################################################################");
        System.out.println("       ########                         #######");
        System.out.println("                成功清空! table_name：" + tb_name);
        System.out.println("       ########                         #######");
        System.out.println("##########################################################################################");


        int SuccessCounter = 0;
        int FailCounter = 0;
        /**爬取文件存放位置**/
        File folder = new File(DataBackupPath + folder_name);

        /**“字段--标签”映射文件规则**/
        File tagRule = new File(tagMappingPath + tb_name + "_tagRule.properties");
        Properties tagRule_props = new Properties();
        tagRule_props.load(new FileReader(tagRule));

        List tagRule_list = new ArrayList(tagRule_props.keySet());

        File[] files = folder.listFiles();
        SAXReader reader = new SAXReader();


        for (File singleFile : files) {
            if (!singleFile.getName().endsWith(".xml")) continue;
            Document dom = reader.read(singleFile);
            Element rootElement = dom.getRootElement();

            /**1.获取所有item标签组成的list*/
            List itemList = rootElement.element(GrandPaTagName).elements(FatherTagName);
            Iterator iterator = itemList.iterator();
            /**2.遍历该list*/
            while (iterator.hasNext()) {
                Element item = (Element) iterator.next();
                Iterator ChildNodes = item.elementIterator();
                HashMap<String, String> map = new HashMap<String, String>();
                while (ChildNodes.hasNext()) {/**item标签内部*/
                    Element child = (Element) ChildNodes.next();
                    if (child.getName().contains("时")) {
                        map.put(child.getName(), child.getText().trim().replaceAll("\'", ""));
                    } else {
                        map.put(child.getName(), child.getText().trim().replaceAll("\'", "").replaceAll("\\s*", ""));
                    }
                }

                String sql_colums = "";
                String values = "";
                for (int i = 0; i < tagRule_list.size(); i++) {
                    if (i == tagRule_list.size() - 1) {
                        sql_colums += tagRule_list.get(i);
                        values += map.get(tagRule_props.getProperty((String) tagRule_list.get(i)));
                    } else {
                        sql_colums += tagRule_list.get(i) + ",";
                        values += map.get(tagRule_props.getProperty((String) tagRule_list.get(i))) + "','";
                    }
                }

                String sql = "insert into " + tb_name + " (uuid," + sql_colums + ",create_time)"
                        + "values('"
                        + UuIdGeneratorUtil.getCetcCloudUuid("31_project") + "','"
                        + values + "','"
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime())
                        + "')";

                pstmt = conn.prepareStatement(sql);
                System.out.println(sql);
                int i = pstmt.executeUpdate();
                if (i > 0) {
                    System.out.println("成功!" + ++SuccessCounter);
                } else {
                    System.out.println("失败!" + ++FailCounter);
                }
                System.out.println("\n成功数：" + SuccessCounter + "-----失败数： " + FailCounter);
            }
        }
    }

    /**
     * 64.案件--右侧小窗内容
     *
     * @throws DocumentException
     * @throws SQLException
     */
    public static void B_Test(String tb_name, String folder_name, String GrandPaTagName, String FatherTagName, String keyName) throws SQLException, IOException, DocumentException {

        int SuccessCounter = 0;
        int FailCounter = 0;
        /**爬取文件存放位置**/
        File folder = new File(DataBackupPath + folder_name);

        /**“字段--标签”映射文件规则**/
        File tagRule = new File(tagMappingPath + tb_name + "_tagRule_B.properties");
        Properties tagRule_props = new Properties();
        tagRule_props.load(new FileReader(tagRule));

        /**映射规则文件key组成的list**/
        List tagRule_list = new ArrayList(tagRule_props.keySet());

        File[] files = folder.listFiles();
        SAXReader reader = new SAXReader();


        for (File singleFile : files) {
            if (!singleFile.getName().endsWith(".xml")) continue;
            Document dom = reader.read(singleFile);
            Element rootElement = dom.getRootElement();

            /**1.获取所有item标签组成的list*/
            List itemList = rootElement.element(GrandPaTagName).elements(FatherTagName);
            Iterator iterator = itemList.iterator();
            /**2.遍历该list*/
            while (iterator.hasNext()) {
                Element item = (Element) iterator.next();
                Iterator ChildNodes = item.elementIterator();
                /** XML标签解析出的  标签名-标签值 组成的map  **/
                HashMap<String, String> map = new HashMap<String, String>();
                while (ChildNodes.hasNext()) {/**item标签内部*/
                    Element child = (Element) ChildNodes.next();
                    if (child.getName().contains("时")) {
                        map.put(child.getName(), child.getText().trim().replaceAll("\'", ""));
                    } else {
                        map.put(child.getName(), child.getText().trim().replaceAll("\'", "").replaceAll("\\s*", ""));
                    }
                }

                String sql_colums = "";

                /**将keyname排除后剩余的标签组成一个list*/
                ArrayList<String> tags_ExceptKey = new ArrayList<String>(tagRule_list);
                tags_ExceptKey.remove(keyName);

                for (int i = 0; i < tags_ExceptKey.size(); i++) {
                    if (tagRule_list.get(i) == keyName) continue;
                    if (i == tags_ExceptKey.size() - 1) {
                        sql_colums += tags_ExceptKey.get(i) + "='" + map.get(tagRule_props.getProperty((String) tags_ExceptKey.get(i))) + "' ";
                    } else {
                        sql_colums += tags_ExceptKey.get(i) + "='" + map.get(tagRule_props.getProperty((String) tags_ExceptKey.get(i))) + "',";
                    }
                }

                String create_time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime());
                String sql = "UPDATE " + tb_name
                        + " SET "
                        + sql_colums
                        + "WHERE " + keyName + "='" + map.get(tagRule_props.getProperty(keyName)) + "'";

                pstmt = conn.prepareStatement(sql);
                System.out.println("执行sql：");
                System.out.println(sql);
                int i = pstmt.executeUpdate();
                if (i > 0) {
                    System.out.println("成功!" + ++SuccessCounter);
                } else {
                    System.out.println("失败!" + ++FailCounter);
                }
                System.out.println("\n成功数：" + SuccessCounter + "-----失败数： " + FailCounter);
            }
        }
    }


}


