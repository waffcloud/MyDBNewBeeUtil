package com.cetc.dom4j;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.io.File;
import java.util.Iterator;
import java.util.List;

public class XmlToObject {

    public static void main(String [] args) {
        try {
//            XmlToObject.test4j();
        } catch (Exception e) {
            e.getStackTrace();
        }
    }
//    public  static void test4j() throws DocumentException {
//
//        ConDb conDb = new ConDb();
//        String tit_1 = "item";
//        int i = 1;
//        int flag = 0;
//        SAXReader reader = new SAXReader();
//
//        while (flag == 0) {
//            String filePath = "src/main/resources/xmlToObject/luo_" + i + ".xml";
//            File file = new File(filePath);
//            if (file.exists()) {
//                i++;
//                Document document;
//                try {
//                    document = reader.read(file);
//                    Element root = document.getRootElement();// 得到根节点
//                    List nodes = root.elements(tit_1);
//                    for (Iterator it = nodes.iterator(); it.hasNext(); ) {
////                        CaseModel caseModel = new CaseModel();
////                        Element elm = (Element) it.next();
////                        for (Iterator it2 = elm.elementIterator(); it2.hasNext(); ) {
////                            Element elel = (Element) it2.next();
////                            System.out.println(elel.getName() + ":" + elel.getText().trim().replaceAll("\\s*", ""));
////                            if ("案例编号".equals(elel.getName())) {
////                                caseModel.setCaseNum(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
////                            if ("案例类型".equals(elel.getName())) {
////                                caseModel.setContent(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
////                            if ("进入环节时间".equals(elel.getName())) {
////                                caseModel.setInTime(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
////                            if ("案件来源".equals(elel.getName())) {
////                                caseModel.setResource(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
////                            if ("上报时间".equals(elel.getName())) {
////                                caseModel.setReportTime(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
////                            if ("区域".equals(elel.getName())) {
////                                caseModel.setArea(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
////                            if ("案件描述".equals(elel.getName())) {
////                                caseModel.setCaseDesc(elel.getText().trim().replaceAll("\\s*", ""));
////                            }
//                        }
//                        String sql = "insert into tbl_case (caseNum,content,inTime,resource,reportTime,area,caseDesc)"
//                                + "values('"
////                                + caseModel.getCaseNum() + "','"
////                                + caseModel.getContent() + "','"
////                                + caseModel.getInTime() + "','"
////                                + caseModel.getResource() + "','"
////                                + caseModel.getReportTime() + "','"
////                                + caseModel.getArea() + "','"
////                                + caseModel.getCaseDesc() + "')";
//                        conDb.con(sql);
////                    }
////                } catch (DocumentException e) {
////                    e.printStackTrace();
////                }
////            }
////    else{
//            flag = 1;
////         }
//        System.out.println("-----------");
//        System.out.println("数据入库完成！");
//        System.out.println("-----------");
////    }
    }
