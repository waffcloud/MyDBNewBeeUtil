package com.cetc.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Description：
 * Created by luolinjie on 2018/4/26.
 */
public class DBUtil {
    /**
     * 获取目标数据库的所有表名（table_name）组成的集合，以便于判断是否需要重新创建表结构
     * （全部转换成小写）
     *
     * @return
     * @throws SQLException
     */
    public static Set getDBAllTableNames(String DB_name, Connection conn_target) throws SQLException {
        Set allTableNameSet = new HashSet();
        String sql = "SELECT table_name from information_schema.TABLES WHERE table_schema = '" + DB_name + "';";
        PreparedStatement pstmt1 = conn_target.prepareStatement(sql);
        ResultSet rs = pstmt1.executeQuery();
        while (rs.next()) {
            String tb_name = rs.getString("table_name");
            allTableNameSet.add(tb_name.toLowerCase());
        }
        if (allTableNameSet.size() > 0) {
            return allTableNameSet;
        } else {
            System.out.println("当前目标库为空!!!");
            return null;
        }
    }


}
