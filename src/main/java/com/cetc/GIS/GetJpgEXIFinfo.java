package com.cetc.GIS;

import com.cetc.model.JpegFileModel;
import com.cetc.util.JdbcUtil;
import com.cetc.util.UuIdGeneratorUtil;
import com.drew.imaging.jpeg.JpegMetadataReader;
import com.drew.imaging.jpeg.JpegProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Description：
 * Created by luolinjie on 2018/4/27.
 */
public class GetJpgEXIFinfo {
    /************************  配置工具 ******************************************/
    /**
     * source:源数据库
     **/
    private static String IP_source = "localhost";
    private static String DB_name_source = "test_xiaofangyinhuan";
    private static String user_source = "root";
    private static String password_source = "123456";
    private static String url1 = "jdbc:mysql://" + IP_source + ":3306/" + DB_name_source + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";

    private static Connection conn_source = null;
    private static PreparedStatement pstmt = null;

    private static String tableName = "tb_fire_danger_img_detail";

    private static final Logger logger = LoggerFactory.getLogger(GetJpgEXIFinfo.class);
    /**
     * 测试用于读取图片的EXIF信息
     *
     * @author Luolinjie
     */

    private static String FolderPath = "E:\\DBBackUp\\201804271539-董鑫给过来隐患照片信息\\隐患照片";

    public static void main(String[] args) throws Exception {

        init();

        readImgInfoAndPutToDB();

        closeConn();
    }

    public static void readImgInfoAndPutToDB() throws JpegProcessingException, IOException, SQLException, ParseException {
        int successCounter = 0;
        int failCounter = 0;

        File folder = new File(FolderPath);
        File[] files = folder.listFiles();
        for (File f : files){
            if (f.isDirectory()){
                File[] files1 = f.listFiles();
                for (File jpgFile: files1){
                    JpegFileModel jpegFileModel = new JpegFileModel();
                    Metadata metadata = JpegMetadataReader.readMetadata(jpgFile);
                    for (Directory directory : metadata.getDirectories()) {
                        for (Tag tag : directory.getTags()) {
                            if ("GPS Latitude".equals(tag.getTagName())) {
                                jpegFileModel.setJd84(translate(tag.getDescription()));
                            }
                            if ("GPS Longitude".equals(tag.getTagName())) {
                                jpegFileModel.setWd84(translate(tag.getDescription()));
                            }
                            if ("Date/Time Original".equals(tag.getTagName())) {
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
                                Date parse = simpleDateFormat.parse(tag.getDescription());
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                String format = sdf.format(parse);
                                jpegFileModel.setShoot_time(format);
                            }
                            jpegFileModel.setLocation(f.getName());
                            jpegFileModel.setDescription(jpgFile.getName().split("\\.")[0]);
                        }

                    }
                    String SQL = "insert into " + tableName+" (uuid,shoot_location,jd84,wd84,shoot_time,description,create_time)"
                            + " values('"
                            + UuIdGeneratorUtil.getCetcCloudUuid("31_project") + "','"
                            + jpegFileModel.getLocation() + "','"
                            + jpegFileModel.getJd84() + "','"
                            + jpegFileModel.getWd84() + "','"
                            + jpegFileModel.getShoot_time() + "','"
                            + jpegFileModel.getDescription() + "','"
                            + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime())
                            + "')";

                    PreparedStatement ps = conn_source.prepareStatement(SQL);
                    int i = ps.executeUpdate();
                    if (i>0){
                        logger.info("成功！"+successCounter++);
                    }else {
                        logger.info("成功！"+failCounter++);
                    }

                }
            }
        }
        logger.info("成功--》"+successCounter+"   ,总失败数--》"+failCounter);
    }
    public static void init() {
        try {
            conn_source = JdbcUtil.getConnection(url1, user_source, password_source);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void closeConn() {
        JdbcUtil.close(conn_source, pstmt);
    }

    /**
     * 转换经纬度进制
     * 举例：22.0° 31.0' 25.56000000000097"
     *      result = 22.0+31.0/60+25.5600000000097/3600
     * @return
     */
    public static String translate(String arg){
        if (arg!=null){
            String[] splits = arg.split(" ");
            double du = Double.parseDouble(splits[0].substring(0, splits[0].length()-1));
            double fen = Double.parseDouble(splits[1].substring(0, splits[1].length()-1));
            double miao = Double.parseDouble(splits[2].substring(0,splits[2].length()-1));

            return String.valueOf(du+fen/60+miao/3600);
        }else {
            return null;
        }
    }
}
