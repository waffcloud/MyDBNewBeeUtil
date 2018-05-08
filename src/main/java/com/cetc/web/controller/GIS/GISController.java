package com.cetc.web.controller.GIS;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cetc.core.util.CoordinateTransformer;
import com.cetc.core.util.HttpUtil;
import com.cetc.core.util.JdbcUtil;
import com.cetc.web.controller.model.JpegFileModel;
import com.drew.imaging.jpeg.JpegMetadataReader;
import com.drew.imaging.jpeg.JpegProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;

@RestController
@RequestMapping("/GIS")
public class GISController {

    private static final Logger logger = LoggerFactory.getLogger(GISController.class);
    private static final String TiandituURL = "http://www.tianditu.com/query.shtml";

    /************
     * 数据库配置参数
     *********/
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;

    private static String IP = "localhost";
    private static String DB_name = "test_xiaofangyinhuan";
    private static String BaseTable = "tb_fire_danger";
    private static String username = "root";
    private static String password = "123456";


    @RequestMapping(value = "/getImageFileAddress", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public JSONObject getImageFileAddress(MultipartFile file) throws IOException, JpegProcessingException, ParseException {
        JpegFileModel jpegFileModel = new JpegFileModel();
                /* 校验:判断待提交文件是否存在 */
        if (file.isEmpty()) {

            JSONObject res = new JSONObject();
            res.put("error", "Empty File!!!");
            return res;
        }
        Metadata metadata = JpegMetadataReader.readMetadata(file.getInputStream());
        for (Directory directory : metadata.getDirectories()) {
            for (Tag tag : directory.getTags()) {
                if ("GPS Longitude".equals(tag.getTagName())) {
                    jpegFileModel.setJd84(translate(tag.getDescription()));
                }
                if ("GPS Latitude".equals(tag.getTagName())) {
                    jpegFileModel.setWd84(translate(tag.getDescription()));
                }
                jpegFileModel.setDescription(file.getName());
            }

        }

        JSONObject locationInfo = getLocationNameByCoordinate(jpegFileModel.getJd84(), jpegFileModel.getWd84());

        JSONObject coordinate = new JSONObject();
        double[] baidu_jd_wd = CoordinateTransformer.wgs84_to_bd09(Double.parseDouble(jpegFileModel.getJd84()), Double.parseDouble(jpegFileModel.getWd84()));
        coordinate.put("jd", baidu_jd_wd[0]);
        coordinate.put("wd", baidu_jd_wd[1]);
        coordinate.put("jd84", jpegFileModel.getJd84());
        coordinate.put("wd84", jpegFileModel.getWd84());
        coordinate.put("coordinate", String.valueOf(baidu_jd_wd[0])+";"
                +String.valueOf(baidu_jd_wd[1])+";"
                +jpegFileModel.getJd84()+";"
                +jpegFileModel.getWd84()
        );

        JSONObject result = new JSONObject();
        result.put("locationInfo", locationInfo);
        result.put("coordinate", coordinate);
        return result;
    }

    @RequestMapping(value = "/fillCoordinateInfo", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String fillCoordinateInfo(String DbName, String targetTable, int id, String coordinateStr) throws SQLException {
        String[] split = coordinateStr.split(";");

        String jd = split[0];
        String wd = split[1];
        String jd84 = split[2];
        String wd84 = split[3];

        String url1 = "jdbc:mysql://" + IP + ":3306/" + DbName + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
        conn = JdbcUtil.getConnection(url1, username, password);


        String SQL = "UPDATE " + targetTable + " " +
                "SET jd=" + jd + ",wd=" + wd + ",jd84=" + jd84 + ",wd84=" + wd84 +
                " WHERE id=" + id;
        logger.info(SQL);
        PreparedStatement statement = conn.prepareStatement(SQL);
        int i = statement.executeUpdate();

        conn.close();
        if (i > 0) {
            return "success!";
        } else {
            return "fail:\r\n" + SQL;
        }


    }

    private JSONObject getLocationNameByCoordinate(String jd84, String wd84) {
        String httpURL = TiandituURL + "?postStr={%27lon%27:" + jd84 + ",%27lat%27:" + wd84 + ",%27appkey%27:8a7b9aac0db21f9dd995e61a14685f05,%27ver%27:1}&type=geocode";

        /**
         * {
         "result": {
         "formatted_address": "广东省深圳市福田区湘赣木桶饭沙尾旗舰店",
         "location": {
         "lon": 114.03541666666666,
         "lat": 22.522341666666417
         },
         "addressComponent": {
         "address": "沙尾路7号附近",
         "city": "广东省深圳市福田区",
         "road": "沙尾路",
         "poi_position": "东南",
         "address_position": "东北",
         "road_distance": 7,
         "poi": "湘赣木桶饭沙尾旗舰店",
         "poi_distance": "7",
         "address_distance": 12
         }
         },
         "msg": "ok",
         "status": "0"
         }
         */
        JSONObject response = HttpUtil.doGet(httpURL);
        String result = response.getString("data");
        JSONObject res = JSON.parseObject(result);

        return res.getJSONObject("result").getJSONObject("addressComponent");
    }

    /**
     * 转换经纬度进制
     * 举例：22.0° 31.0' 25.56000000000097"
     * result = 22.0+31.0/60+25.5600000000097/3600
     *
     * @return
     */
    public static String translate(String arg) {
        if (arg != null) {
            String[] splits = arg.split(" ");
            double du = Double.parseDouble(splits[0].substring(0, splits[0].length() - 1));
            double fen = Double.parseDouble(splits[1].substring(0, splits[1].length() - 1));
            double miao = Double.parseDouble(splits[2].substring(0, splits[2].length() - 1));

            return String.valueOf(du + fen / 60 + miao / 3600);
        } else {
            return null;
        }
    }

}