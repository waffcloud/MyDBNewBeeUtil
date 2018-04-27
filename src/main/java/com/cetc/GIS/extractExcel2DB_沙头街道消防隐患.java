package com.cetc.GIS;

import com.cetc.util.JdbcUtil;
import com.cetc.util.UuIdGeneratorUtil;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Description：
 * Created by luolinjie on 2018/2/3.
 */
public class extractExcel2DB_沙头街道消防隐患 {

    private static Connection conn = null;
    private static PreparedStatement pstmt = null;

    private static String IP = "192.168.16.54";
    private static String DB_name = "31project_april";
    private static String username = "root";
    private static String password = "123456";

    private static String url1 = "jdbc:mysql://"+IP+":3306/"+DB_name+"?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=convertToNull";



    private static int START_ROW = 3;//Default:3
    private static String BaseTable = "tb_shops_fire_danger";
    private static String AllColunms = "uuid,address,jd,wd,jd84,wd84,unit,danger_type,danger_grade,juridical_person," +
            "director,responsible_police,leader,work_place,reform_measures,time_limit,progress," +
            "create_time";
    private static long counter = 1;
    private static String FileNameStr = "C:\\Users\\Administrator\\Desktop\\2018年金地工业区第一季度企业商铺检查.xls";

    @Test
    public void extractExcel2DB() throws Exception {
        HSSFWorkbook workbook = new HSSFWorkbook(new FileInputStream(new File(FileNameStr)));
        HSSFSheet sheet = null;
        //获取当前时间字符串
        Calendar instance = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formatedTime = simpleDateFormat.format(instance.getTime());

//        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {// 获取每个Sheet表
            sheet = workbook.getSheetAt(0);
            for (int j = START_ROW; j < sheet.getLastRowNum() + 1; j++) {// getLastRowNum，获取最后一行的行标
                HSSFRow row = sheet.getRow(j);
                String uuid = UuIdGeneratorUtil.getCetcCloudUuid("31_project");
                double ExcelId = row.getCell(0).getNumericCellValue();
                System.out.println("====== ExcelId: ====="+ExcelId);
                String address = row.getCell(1).getStringCellValue();
                HSSFCell cell2 = row.getCell(2);
                String organization_name =null;
                if (HSSFCell.CELL_TYPE_STRING==cell2.getCellType()) {
                    organization_name = cell2.getStringCellValue();
                }else if (HSSFCell.CELL_TYPE_NUMERIC==cell2.getCellType()){
                    organization_name = String.valueOf(cell2.getNumericCellValue()).split("\\.")[0];
                }
                String owner_name = row.getCell(3).getStringCellValue();

                String check_date=null;
                HSSFCell check_date_cell = row.getCell(4);
                if (HSSFCell.CELL_TYPE_STRING == check_date_cell.getCellType()){
                    check_date = transDateTime(check_date_cell.getStringCellValue());
                }else if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(check_date_cell)) {
                    Date check_date_date = check_date_cell.getDateCellValue();
                    check_date_date.setHours(12);
                    check_date = simpleDateFormat.format(check_date_date);
                }

                String risks = row.getCell(5).getStringCellValue();

                String review_date=null;
                HSSFCell review_date_cell = row.getCell(6);
                if (HSSFCell.CELL_TYPE_STRING == review_date_cell.getCellType()){
                    review_date = transDateTime(review_date_cell.getStringCellValue());
                }else if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(review_date_cell)) {
                    Date review_date_date = review_date_cell.getDateCellValue();
                    review_date_date.setHours(12);
                    review_date = simpleDateFormat.format(review_date_date);
                }

                String review_risks = row.getCell(7).getStringCellValue();
                String comments = row.getCell(8).getStringCellValue();

                String sql = "INSERT INTO " + BaseTable + " (" + AllColunms + ") VALUES('"
                        + uuid + "','"
                        + organization_name + "','"
                        + address + "','"
                        + owner_name + "','"
                        + check_date + "','"
                        + risks + "','"
                        + review_date + "','"
                        + review_risks + "','"
                        + comments + "','"
                        + formatedTime + "')";
                // 3.创建statment
                pstmt = conn.prepareStatement(sql);
                System.out.println("preparing sql:" + sql);

                // 5.执行sql语句，得到返回结果
                int count = pstmt.executeUpdate(sql);
                System.out.println("本次执行共影响了：" + count + "行数据, 插入数据总数：" + counter++);
            }
            System.out.println(""); // 读完一行后换行
        System.out.println("读取sheet表：" + sheet.getSheetName() + " 完成");
    }

//    }

    // 读取，指定sheet表及数据
    @Before
    public void init() throws Exception {
        conn = JdbcUtil.getConnection();
    }

    @After
    public void close() throws Exception {
        conn.close();
    }


    public String transDateTime(String dateString){
        if (null==dateString || "".equals(dateString)){
            throw new RuntimeException("transDateTime():Can not transmit dateString:'"+dateString+"'");
        }
        String[] splits = dateString.split("\\.");
        String year = splits[0];
        String month = splits[1];
        String day = splits[2];

        if (month.length()<2){
            month = "0"+month;
        }
        if (day.length()<2){
            day = "0"+day;
        }

        String returnDateString = year+"-"+month+"-"+day+" 12:00:00";
        return returnDateString;
    }
}
