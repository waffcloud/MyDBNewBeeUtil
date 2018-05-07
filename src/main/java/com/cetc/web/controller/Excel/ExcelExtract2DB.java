package com.cetc.web.controller.Excel;

import com.cetc.core.util.JdbcUtil;
import com.cetc.core.util.UuIdGeneratorUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Description：
 * Created by luolinjie on 2018/2/3.
 */
@RestController
@RequestMapping("/Excel")
public class ExcelExtract2DB {
    private static final Logger logger = LoggerFactory.getLogger(ExcelExtract2DB.class);
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;

    private static String IP = "localhost";
    private static String DB_name = "test_xiaofangyinhuan";
    private static String BaseTable = "tb_fire_danger";
    private static String username = "root";
    private static String password = "123456";
    private static int START_ROW = 3;//Default:3

    private static String AllColunms = "uuid,address,city,region,street,community,danger_pattern,danger_type,danger_description,photo_url," +
            "reform_measures,responsibility_unit,reform_time_limit,reform_progress," +
            "create_time";
    private static long counter = 0;

    @RequestMapping(value = "/extractExcelInfo2DB", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String extractExcel2DB(MultipartFile file, String Data_StartRow_Num) throws Exception {
        String url1 = "jdbc:mysql://" + IP + ":3306/" + DB_name + "?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";
        conn = JdbcUtil.getConnection(url1, username, password);
        int startRow = 1;
        if (null == Data_StartRow_Num || "".equals(Data_StartRow_Num)) {
            startRow = 1;
        } else {
            startRow = Integer.valueOf(Data_StartRow_Num) - 1;
        }

        Workbook workbook = null;
        String originalFilename = file.getOriginalFilename();
        if (originalFilename.endsWith(".xls")) {
            workbook = new HSSFWorkbook(file.getInputStream());
        } else if (originalFilename.endsWith(".xlsx")) {
            workbook = new XSSFWorkbook(file.getInputStream());
        }
        //获取当前时间字符串
        Calendar instance = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formatedTime = simpleDateFormat.format(instance.getTime());

        // TODO：获取第一个sheet
        Sheet sheet = workbook.getSheetAt(0);
        // todo：getLastRowNum，获取最后一行的行标
        logger.debug(String.valueOf(sheet.getLastRowNum()));
        for (int j = startRow; j < sheet.getLastRowNum(); j++) {
            Row row = sheet.getRow(j);
            if (null == row.getCell(0).getStringCellValue() || "".equals(row.getCell(0).getStringCellValue())) {
                break;
            }
            String uuid = UuIdGeneratorUtil.getCetcCloudUuid("31_project");
            String address = row.getCell(0).getStringCellValue();
            String city = row.getCell(1).getStringCellValue();
            String region = row.getCell(2).getStringCellValue();
            String street = row.getCell(3).getStringCellValue();
            String community = row.getCell(4).getStringCellValue();
            String danger_pattern = row.getCell(5).getStringCellValue();
            String danger_type = row.getCell(6).getStringCellValue();
            String danger_description = row.getCell(8).getStringCellValue();
            String photo_url = row.getCell(9).getStringCellValue();
            String reform_measures = row.getCell(10).getStringCellValue();
            String responsibility_unit = row.getCell(11).getStringCellValue();
            String reform_time_limit = row.getCell(17).getStringCellValue();
            String reform_progress = row.getCell(18).getStringCellValue();
            String create_time = simpleDateFormat.format(row.getCell(19).getDateCellValue());

            String sql = "INSERT INTO " + BaseTable + " (" + AllColunms + ") VALUES('"
                    + uuid + "','"
                    + address + "','"
                    + city + "','"
                    + region + "','"
                    + street + "','"
                    + community + "','"
                    + danger_pattern + "','"
                    + danger_type + "','"
                    + danger_description + "','"
                    + photo_url + "','"
                    + reform_measures + "','"
                    + responsibility_unit + "','"
                    + reform_time_limit + "','"
                    + reform_progress + "','"
                    + create_time + "')";
            // 3.创建statment
            pstmt = conn.prepareStatement(sql);

            logger.info("preparing sql:" + sql);

            // 5.执行sql语句，得到返回结果
            int count = pstmt.executeUpdate(sql);
            logger.info("本次执行共影响了：" + count + "行数据, 插入数据总数：" + counter++);
        }

        logger.info("读取sheet表：" + sheet.getSheetName() + " 完成");
        conn.close();
        return "插入数据总数：" + counter;
    }


    // 读取，指定sheet表及数据
    public void init() throws Exception {
        conn = JdbcUtil.getConnection();
    }

    public void close() throws Exception {
        conn.close();
    }


    public String transDateTime(String dateString) {
        if (null == dateString || "".equals(dateString)) {
            throw new RuntimeException("transDateTime():Can not transmit dateString:'" + dateString + "'");
        }
        String[] splits = dateString.split("\\.");
        String year = splits[0];
        String month = splits[1];
        String day = splits[2];

        if (month.length() < 2) {
            month = "0" + month;
        }
        if (day.length() < 2) {
            day = "0" + day;
        }

        String returnDateString = year + "-" + month + "-" + day + " 12:00:00";
        return returnDateString;
    }
}
