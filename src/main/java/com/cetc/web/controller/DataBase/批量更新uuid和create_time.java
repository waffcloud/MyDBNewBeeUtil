package com.cetc.core.database;

import com.cetc.core.util.JdbcUtil;
import com.cetc.core.util.UuIdGeneratorUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Description：
 * Created by luolinjie on 2018/3/16.
 */
public class 批量更新uuid和create_time {

    //数据库ip    //用户名   //密码    //数据库名称
//    private static String db_ip = "192.168.16.103";
//    private static String user = "root";
//    private static String password = "pingtaizu972";
//    private static String db_name = "db_31project";

    //31_project_alpha 数据库ip    //用户名   //密码
//    private static String db_ip = "10.0.12.189";
//    private static String user = "root";
//    private static String password = "centos";
//    private static String db_name = "db_31project_alpha";

//    31_project_april 数据库ip    //用户名   //密码  //数据库名称
    private static String db_ip = "192.168.16.220";
    private static String user = "root";
    private static String password = "123456";
    private static String db_name = "31project_april";

    private static String url = "jdbc:mysql://"+db_ip+":3306/" + db_name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;

    public static void main(String[] args) throws Exception {

        init();
//        printAllTables(db_name);
//        updateUuIdAndCreateTime(db_name);
        updateUuId("xiaohuoshuan_001");
//        updateUuId("XIAOFANGZHAN_001");
        closeConn();
    }

    public static void init() {
        try {
            conn = JdbcUtil.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn, pstmt);
    }



    public static void printAllTables(String db_name) throws Exception {
        //todo: 1.获取所有表的表名
        String sql0 = "SELECT table_name FROM information_schema.TABLES WHERE table_schema='" + db_name + "'";
        pstmt = conn.prepareStatement(sql0);

        ResultSet rs = pstmt.executeQuery();
        int count = 0;
        while (rs.next()){
            String tb_name = rs.getString(1);
            System.out.println(tb_name);
            count++;
        }
        System.out.println("\r\n数据库"+db_name+"总共有"+count+"张表");

    }
    public static void updateUuIdAndCreateTime(String db_name) throws Exception {

      /*  String[] targetTables = {
//                "ANQUANBIAOZHUNHUAQIYE_001",
//                "ANQUANSHENGCHANTJ_001",
//                "ANQUANSHIGU_001",
//                "BINANCHANGSUO_FENQU_TJ_001",
//                "BINANCHANGSUO_001",
//                "CHENGSHITIZHENG_TJ_001",
//                "CHULIZHONGAJ_001",
//                "DAIBANJIEAJ_001",
//                "DAIPAIQIANAJ_001",
//                "DAISHOULIAJ_001",
//                "DAOLUYANGHU_001",
//                "DASHAGUANGCHANG_001",
//                "DITIEZHAN_001",
//                "DIXIANDIAN_001",
//                "DUICHANG_001",
                "DUIXIANGFENLEI_001",
                "FANGFENYUJING_001",
                "FANGWULEIXINGTJ_001",
                "FARENHANGYETJ_001",
                "FARENQUYUTJ_001",
                "FENCHENSHEBAOQIYE_001",
                "FENGLIJIANCEZIDONGZHAN_005",
                "GANGKOUMATOU_001",
                "GAOTIEHUOCHEZHAN_001",
                "GONGCHENGJIANZHU_001",
                "HAIDI_001",
                "HAIQUFENGLIJIANCEZIDONGJIANCE_005",
                "HAIQUFENGLIJIANCEZIDONGZHAN_005",
                "HEDAOYUJING_TJ_001",
                "HEDAOYUSHUIQING_001",
                "HEDAO_001",
                "HEDIANZHAN_001",
                "HUAPODIAN_001",
                "JIANZHUWU_001",
                "JIAOTONGDAOLU_TJ_001",
                "JIAOYUJIGOU_001",
                "JILAODIAN_001",
                "JILAODIAN_YUJING_TJ_001",
                "JILAOZAIQING_001",
                "JILAOZAIQING_FENQU_TJ_001",
                "JILAOZAIQING_FENSHI_TJ_001",
                "JISHUIBENGZHAN_001",
                "JISHUICHANG_001",
                "JISHUILUDUAN_001",
                "JIUYUANGONGXU_TJ_001",
                "KOUAN_001",
                "LVYOUJINGDIAN_001",
                "NIANDUNASHUIDAHU_001",
                "PAISHUIBENGZHAN_001",
                "QIANGXIANDUIWU_001",
                "QIANSHUIQIYE_001",
                "QICHEZHAN_001",
                "QIXIANGJIANCEZHAN_001",
                "QIXIANGJIANCE_001",
                "QIXIANGYUJING_001",
                "QIYEBULIANGJILU_001",
                "QIYEHEIMINGDAN_001",
                "QUSHUIDIAN_001",
                "RANQIZHAN_001",
                "RENKOUFENQUTJ_001",
                "RENKOUJIEGOUTJ_001",
                "SANFANGWUZI_001",
                "SANFANGYUJINGXINXI_001",
                "SENLINHUOZAI_001",
                "SHANGSHIZHUTI_001",
                "SHEQUANQUANZHISHU_TJ_001",
                "SHIPIN_001",
                "SHUIKUYUJING_TJ_001",
                "SHUIKUYUSHUIQING_001",
                "SHUIKU_001",
                "SHUIZHA_001",
                "TAIFENGLUJING_001",
                "TIANRANQUSHUIDIAN_001",
                "TIYUCHANGSUO_001",
                "TUIHUIAJ_001",
                "WANGLUOYUQING_001",
                "WANGLUOYUQING_TJ_001",
                "WEIHUADIAN_001",
                "WEIHUAPINQIYE_001",
                "WEIHUAPINXUKEYUJING_001",
                "WEIHUO_BAOJINGTJ_001",
                "WEIHUO_CHAOSUTJ_001",
                "WEIHUO_CHELIANGJIANKONG_001",
                "WEIHUO_DIAOXIANTJ_001",
                "WEIHUO_JSYYSQKFX_001",
                "WEIHUO_QIYE_001",
                "WEIHUO_RENYUAN_001",
                "WEIHUO_SHANGXIANTJ_001",
                "WEIHUO_TINGCHECHANG_001",
                "WEIHUO_TONGJI_001",
                "WEIHUO_YUNDAN_001",
                "WENHUACHANGSUO_001",
                "WENTIZHONGXIN_001",
                "WURANQIYE_001",
                "WUZICANGKU_001",
                "XIAOFANGSHESHI_TJ_001",
                "XIAOFANGSHIJIAN_TJ_001",
                "XIAOFANGWUZHI_001",
                "XIAOFANGYINHUANSHIJIAN_TJ_001",
                "XIAOFANGZAIQING_001",
                "XIAOFANGZHAN_001",
                "XIAOHUOSHUAN_001",
                "XINGZHENGQU_001",
                "YANSHIHEZHUN_001",
                "YIBANJIEAJ_001",
                "YILIAOJIGOU_001",
                "YIZHIDUHUAPINQIYE_001",
                "YOUKU_001",
                "YOUQIZHAN_001",
                "ZHONGDAWEIXIANYUAN_001",
                "ZHONGDAYINHUAN_001",
                "ZHONGDIANQUYURENLIU_TJ_001"
        };*/
        String[] targetTables = {
                "video_categories"
        };

        //todo: 2.遍历所有表，执行更新操作
        int count = 0;
        for (String tb_name:targetTables) {
            System.out.println("\r\n\r\n正在处理表：---------  " + tb_name + "  ------------");
            int i1 = updateUuId(tb_name);
//            int i2 = updateCreateTime(tb_name);
            System.out.println("更新" + tb_name + "--uuid条数：---------  " + i1 + "  ------------");
//            System.out.println("更新" + tb_name + "--create_time update_time条数：---------  " + i2 + "  ------------");
            count += i1;
            System.out.println("Total:"+count);
        }

    }


    public static int updateUuId(String BaseTable) throws Exception {

        //todo:1. 查询当前表id集合
        String sql_queryIds = "SELECT id from " + BaseTable;

        //备选: 按条件查询,uuid为非空的才执行更新
//        String sql_queryIds = "SELECT id from " + BaseTable +" WHERE UUID=''";
        pstmt = conn.prepareStatement(sql_queryIds);
        ResultSet resultSet = pstmt.executeQuery();

        int counter = 0;
        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String sql_updateUuid = "UPDATE " + BaseTable + " set uuid='" + UuIdGeneratorUtil.getCetcCloudUuid("db_31project")
                    + "' where id=" + id;
            pstmt = conn.prepareStatement(sql_updateUuid);
            System.out.println("preparing sql:" + sql_updateUuid);

            int count = pstmt.executeUpdate(sql_updateUuid);
            if (count > 0) {
                System.out.println("更新uuid，影响" + count + "行数据, 插入数据总数：" + counter++);
            }
        }
        return counter;

    }


    public static int updateCreateTime(String BaseTable) throws Exception {
        String timeStr = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime());
        int counter = 0;
        String sql = "UPDATE " + BaseTable + " SET update_time='" + timeStr
                + "',create_time='" + timeStr + "'";
        pstmt = conn.prepareStatement(sql);
//        System.out.println("preparing sql:" + sql);

        // 5.执行sql语句，得到返回结果
        int count = pstmt.executeUpdate(sql);
//        System.out.println("updateCreateTime() ：" + count + "行数据, 插入数据总数：" + counter++);
        if (count > 0) {
            counter++;
        }
        return counter;
    }

}
