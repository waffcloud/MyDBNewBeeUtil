package com.cetc.web.controller.DataBase;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cetc.core.util.DBUtil;
import com.cetc.core.util.JdbcUtil;
import com.cetc.core.util.UuIdGeneratorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * Description： 数据库交互接口
 * Created by luolinjie on 2018/5/8.
 */
@RestController
public class DataBaseController {

    private static final Logger logger = LoggerFactory.getLogger(DataBaseController.class);
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;

    private static String IP = "localhost";
    private static String username = "root";
    private static String password = "123456";
    private static String DB_name = "test";
    private static Properties properties;

    {
        properties = new Properties();
        InputStream inputStream = DataBaseController.class.getResourceAsStream("/columnNameRule.properties");
        try {
            properties.load(new InputStreamReader(inputStream));
        } catch (IOException e) {
            logger.error("could not load file: columnNameRule.properties");
            e.printStackTrace();
        }
    }

    /**
     * 为配置参数重新赋值
     *
     * @param IP
     * @param DB_name
     * @param username
     * @param password
     * @return
     * @throws SQLException
     */
    @RequestMapping(value = "/reInitConnectionProperties", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String reInitConnectionProperties(String IP, String DB_name, String username, String password) throws SQLException {
        DataBaseController.IP = IP;
        DataBaseController.DB_name = DB_name;
        DataBaseController.username = username;
        DataBaseController.password = password;

        JSONObject res = new JSONObject();
        res.put("IP", DataBaseController.IP);
        res.put("DB_name", DataBaseController.DB_name);
        res.put("username", DataBaseController.username);
        res.put("password", DataBaseController.password);

        return res.toJSONString();
    }

    @RequestMapping(value = "/batchUpdateUuId", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public int BatchUpdateUuId(String BaseTable) throws Exception {
        init(DB_name);
        //todo:1. 查询当前表id集合
        String sql_queryIds = "SELECT id from " + BaseTable;

        pstmt = conn.prepareStatement(sql_queryIds);
        ResultSet resultSet = pstmt.executeQuery();

        int counter = 0;
        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String sql_updateUuid = "UPDATE " + BaseTable + " set uuid='" + UuIdGeneratorUtil.getCetcCloudUuid("db_31project")
                    + "' where id=" + id;
            pstmt = conn.prepareStatement(sql_updateUuid);
            logger.info("Preparing SQL:" + sql_updateUuid);

            int count = pstmt.executeUpdate(sql_updateUuid);
            if (count > 0) {
                logger.info("更新uuid，影响" + count + "行数据, 插入数据总数：" + ++counter);
            }
        }
        return counter;

    }

    /**
     * 将指定数据库的所有表字段由大写转换为小写
     * @return
     * @throws SQLException
     */
    @RequestMapping(value = "/parseDBAllColumnNameToLowerCase", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String parseDBAllColumnNameToLowerCase() throws SQLException {
        init(DB_name);
        JSONObject res = new JSONObject();
        //todo:1 获取当前数据库的所有表名组成的列表
        Set set = DBUtil.getDBAllTableNames(DB_name, conn);
        Iterator iterator = set.iterator();

        while (iterator.hasNext()) {
            String tableName = (String) iterator.next();
            //todo:2 获取当前表所有字段名组成的列表
            String SQL_getAllColumns = "SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT FROM `information_schema`.`columns` " +
                    "WHERE `table_schema` = '" + DB_name + "'" +
                    "AND `table_name` = '" + tableName + "'";
            PreparedStatement ps1 = conn.prepareStatement(SQL_getAllColumns);
            ResultSet rs1 = ps1.executeQuery();
            JSONArray array = new JSONArray();
            while (rs1.next()) {
                //todo:3 获取字段,并获取其字段类型
                String columnName = rs1.getString(1);
                String columnType = rs1.getString(2);
                String columnComment = rs1.getString(3);

                //如果字段名称已经是小写的话，就不做修改
                if (columnName.equals(columnName.toLowerCase())) {
                    continue;
                }
                logger.info("colunmName --> " + tableName + "." + columnName);
                //alter table 表名  change 旧字段名 新字段名 新数据类型
                String sql2 = "ALTER TABLE " + tableName + " CHANGE `" + columnName + "` `"
                        + columnName.toLowerCase() + "` " + columnType + " COMMENT '" + columnComment + "'";
                PreparedStatement ps2 = conn.prepareStatement(sql2);
                boolean execute = ps2.execute();
                logger.info(sql2);
                array.add(columnName);
            }
            res.put(tableName, array);
        }

        return res.toJSONString();
    }

    /**
     * @param DB_name
     * @param tableName
     * @return
     * @throws SQLException
     */
    @RequestMapping(value = "/checkTableColumnNameRule", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String checkTableColumnNameRule(String DB_name, String tableName) throws SQLException, IOException {
        init(DataBaseController.DB_name);
        JSONObject result = new JSONObject();
        Set<Object> set = properties.keySet();

        //todo:0.获取目标表的字段组成的集合
        String SQL_getAllColumns = "SELECT COLUMN_NAME,COLUMN_TYPE,COLUMN_COMMENT FROM `information_schema`.`columns` " +
                "WHERE `table_schema` = '" + DB_name + "'" +
                "AND `table_name` = '" + tableName + "'";
        PreparedStatement ps1 = conn.prepareStatement(SQL_getAllColumns);
        ResultSet rs1 = ps1.executeQuery();
        while (rs1.next()) {
            String columnName = rs1.getString(1);
            set.add(columnName);
            String columnType = rs1.getString(2);
            String columnComment = rs1.getString(3);
        }

        //todo:1.检查字段名是否为全小写


        //todo:2.检查该表中是否包含Base字段，以及对应的字段类型是否一致(通过配置文件读取)


        return result.toJSONString();
    }


    /**
     * 以分隔符拼接的方式，打印出该表的所有字段名: col1,col2...
     *
     * @param DB_name
     * @param tableName
     * @param separator
     * @return
     * @throws SQLException
     */
    @RequestMapping(value = "/printColumsWithSeperator", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String printColumsWithSeperator(String DB_name, String tableName, String separator) throws SQLException {

        init(DB_name);

        JSONObject result = new JSONObject();

        String sql = "SELECT COLUMN_NAME FROM " +
                "information_schema.columns " +
                "WHERE " +
                "table_schema='" + DB_name +
                "' AND  table_name='" + tableName + "'";
        logger.info(sql);
        PreparedStatement statement = conn.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        StringBuffer sb = new StringBuffer();
        while (resultSet.next()) {
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


    /**
     * 统计每张表中的记录总数 以 table_name,table_rows,table_comment形式打印
     *
     * @param dbName
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/countDBAllTables", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public JSONObject countDBAllTables(String dbName) throws Exception {

        init(dbName);

        JSONObject result = new JSONObject();

        String SQL = "SELECT \n" +
                "table_name,table_rows,table_comment\n" +
                "FROM \n" +
                "information_schema.tables \n" +
                "WHERE \n" +
                "TABLE_SCHEMA = '" + dbName + "' \n" +
                "ORDER BY \n" +
                "table_rows DESC;";
        PreparedStatement statement = conn.prepareStatement(SQL);
        ResultSet resultSet = statement.executeQuery();
        System.out.println("|--table_name--|--table_rows--|--table_comment--|");
        int i = 1;
        while (resultSet.next()) {
            String table_name = resultSet.getString(1);
            int table_rows = resultSet.getInt(2);
            String table_comment = resultSet.getString(3);
            System.out.println(table_name + "\t" + table_rows + "\t" + table_comment);
            JSONObject cell = new JSONObject();
            cell.put("table_name", table_name);
            cell.put("table_rows", table_rows);
            cell.put("table_comment", table_comment);
            result.put(String.valueOf(i++), cell);
        }
        return result;
    }

    /**
     * 检查指定数据库哪张表中不存在指定字段
     * @param dbName
     * @param columnName
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/checkTbNotExistsColunm_printTBName", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public HashSet<String> checkTbNotExistsColunm_printTBName(String dbName, String columnName) throws Exception {

        HashSet<String> result = new HashSet<String>();

        // todo:1.获取所有表名组成的集合

        Set<String> allTables = getAllTables(dbName);
        System.out.println("以下表不包含字段：" + columnName);
        for (String singleTable : allTables) {
            Set<String> tbAllColums = getTbAllColums(dbName, singleTable);
            if (!tbAllColums.contains(columnName)) {
                result.add(singleTable);
                System.out.println(singleTable);
            }
        }


        System.out.println("check over：checkTbNotExistsColunm_printTBName()--" + columnName);
        return result;
    }

    /**
     * 获取一个表中所有的字段名组成的集合
     *
     * @param DB_name
     * @param tableName
     * @return
     * @throws SQLException
     */
    public Set<String> getTbAllColums(String DB_name, String tableName) throws SQLException {
        init(DB_name);
        String sql = "SELECT COLUMN_NAME FROM " +
                "information_schema.columns " +
                "WHERE " +
                "table_schema='" + DB_name +
                "' AND  table_name='" + tableName + "'";
        logger.info(sql);
        PreparedStatement statement = conn.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();

        Set<String> columnSet = new HashSet<String>();
        while (resultSet.next()) {
            columnSet.add(resultSet.getString(1));
        }

        logger.info(columnSet.toString());
        conn.close();
        return columnSet;
    }

    /**
     * 获取一个数据库中所有表名组成的集合
     *
     * @param DB_name
     * @return
     * @throws SQLException
     */
    public Set<String> getAllTables(String DB_name) throws SQLException {
        Set allTableNameSet = new HashSet();
        init(DB_name);
        String sql = "SELECT table_name from information_schema.TABLES WHERE table_schema = '" + DB_name + "';";
        PreparedStatement pstmt1 = conn.prepareStatement(sql);
        ResultSet rs = pstmt1.executeQuery();
        while (rs.next()) {
            String tb_name = rs.getString("table_name");
            allTableNameSet.add(tb_name.toLowerCase());
        }

        logger.info(allTableNameSet.toString());
        conn.close();
        return allTableNameSet;
    }


    /**
     * 为目标表增加指定字段
     *
     * @param dbName
     * @param tableName
     * @param newColumnName
     * @return
     */
    @RequestMapping(value = "/addColumn", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String addColumn(String dbName, String tableName, String newColumnName, String columnType, String defaultValue, String comment) throws SQLException {
        init(dbName);
        JSONObject result = new JSONObject();

        try {
            String SQL = "alter table " + tableName + " add "
                    + newColumnName + " "
                    + columnType
                    + " DEFAULT " + defaultValue
                    + " comment '" + comment + "'";
            PreparedStatement statement = conn.prepareStatement(SQL);
            statement.execute();
            result.put("result", "success! SQL:" + SQL);
            return result.toJSONString();
        } catch (Exception e) {
            result.put("result", "fail!");
            logger.error(e.toString());
            return result.toJSONString();
        }
    }

    /**
     * 批量修改表结构
     *
     * @param tableNames    表名集合，默认以","作为分隔符
     * @param dbName
     * @param newColumnName
     * @param columnType
     * @param comment
     * @return
     * @throws SQLException
     */
    @RequestMapping(value = "/batchAddColumn", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String batchAddColumn(String tableNames, String dbName, String newColumnName, String columnType, String defaultValue, String comment) throws SQLException {
        init(dbName);
        JSONObject result = new JSONObject();

        String[] tables = tableNames.split(",");
        for (String table : tables) {
            String s = addColumn(dbName, table, newColumnName, columnType, defaultValue, comment);
            JSONObject res1 = new JSONObject();
            res1.put("table", table);
            res1.put("return", s);
            result.put("table", res1);
        }
        return result.toJSONString();
    }

    private static void init(String DB_name) {
        String url1 = "jdbc:mysql://" + IP + ":3306/" + DB_name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
        conn = JdbcUtil.getConnection(url1, username, password);
    }
}
