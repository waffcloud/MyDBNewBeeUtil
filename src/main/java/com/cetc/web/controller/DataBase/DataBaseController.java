package com.cetc.web.controller.DataBase;

import com.alibaba.fastjson.JSONObject;
import com.cetc.core.util.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Description： 数据库交互接口
 * Created by luolinjie on 2018/5/8.
 */
@RestController
public class DataBaseController {

    private static final Logger logger = LoggerFactory.getLogger(DataBaseController.class);
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;

    private static String IP = "192.168.16.220";
    private static String username = "root";
    private static String password = "123456";

    @RequestMapping(value = "/printColums", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String printColums(String DB_name,String tableName,String separator) throws SQLException {
        JSONObject result = new JSONObject();

        String url1 = "jdbc:mysql://" + IP + ":3306/" + DB_name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
        conn = JdbcUtil.getConnection(url1, username, password);

        String sql = "SELECT COLUMN_NAME FROM " +
                "information_schema.columns " +
                "WHERE " +
                "table_schema='"+DB_name+
                "' AND  table_name='"+tableName+"'";
        logger.info(sql);
        PreparedStatement statement = conn.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        StringBuffer sb = new StringBuffer();
        while (resultSet.next()){
            sb.append(resultSet.getString(1));
            if (!resultSet.isLast()) {
                sb.append(separator);
            }
        }

        logger.info(sb.toString());
        conn.close();
        result.put("data", sb.toString());
        return result.toJSONString();
    }
}
