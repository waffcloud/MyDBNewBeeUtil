package com.cetc.core.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class ESOperate {

    public static ObjectMapper objectMapper = new ObjectMapper();


    public static String getIndexName(String dbName, String tbName){
        return tbName+"@"+dbName;
    }


    /**
     * 查询接口1：根据一个字段查询另一个字段。使用_source作为过滤器
     * @param indexName 索引名
     * @param outputParam 返回字段，该字段一般用于关联其他索引
     * @param inputParam 查询字段
     * @param inputStr 查询字段must匹配值
     * @return List<Object> 一般来说是List<String>，但为了兼容对象字段，例如loaction
     * @throws IOException
     */
    public static List<Object> queryFieldByField(String indexName, String outputParam, String inputParam, String inputStr) throws IOException {
        String body =  "{\n" +
                        "\t\"_source\":{\n" +
                        "\t\t\"include\":[\""+outputParam+"\"]\n" +
                        "\t},\n" +
                        "\t\"query\":{\n" +
                        "\t\t\"bool\":{\n" +
                        "\t\t\t\"must\":{\n" +
                        "\t\t\t\t\"match\":{\n" +
                        "\t\t\t\t\t\""+inputParam+"\":\""+inputStr+"\"\n" +
                        "\t\t\t\t}\n" +
                        "\t\t\t}\n" +
                        "\t\t}\n" +
                        "\t}\n" +
                        "}";

        /*根据buildingName查询个人身份证号码*/
        MyURLConnection myURLConnection = new MyURLConnection();
        String result= myURLConnection.request(
                MyConstParam.esURL+indexName+"/_search",
                "POST",
                body);
        LinkedHashMap listMap = objectMapper.readValue(result, LinkedHashMap.class);

        List<LinkedHashMap> linkedHashMaps = extractQueryResult(listMap);

        List<Object> resultValues = new ArrayList<Object>();
        for (LinkedHashMap linkedHashMap: linkedHashMaps){
            resultValues.add(linkedHashMap.get(outputParam));
        }
        return resultValues;
    }

    /**
     * 查询接口1：根据一个字段查询另一个字段。使用_source作为过滤器。使用scroll游标查询，最后一个值为scroll_id
     * @param indexName 索引名
     * @param outputParam 返回字段，该字段一般用于关联其他索引
     * @param inputParam 查询字段
     * @param inputStr 查询字段must匹配值
     * @param size 返回查询结果的数量 1-10000
     * @return List<Object> 一般来说是List<String>，但为了兼容对象字段，例如loaction
     * @throws IOException
     */
    public static List<Object> queryFieldByField(String indexName, String outputParam, String inputParam, String inputStr, int size) throws Exception {
        if(size>10000 || size < 1)throw new Exception("size can't be more than 10000, or less than 1\n") ;
        String body =  "{\n" +
                "\"size\":\""+size+"\"," +
                "\t\"_source\":{\n" +
                "\t\t\"include\":[\""+outputParam+"\"]\n" +
                "\t},\n" +
                "\t\"query\":{\n" +
                "\t\t\"bool\":{\n" +
                "\t\t\t\"must\":{\n" +
                "\t\t\t\t\"match\":{\n" +
                "\t\t\t\t\t\""+inputParam+"\":\""+inputStr+"\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        /*根据buildingName查询个人身份证号码*/
        MyURLConnection myURLConnection = new MyURLConnection();
        String result= myURLConnection.request(
                MyConstParam.esURL+indexName+"/_search?scroll=10m",
                "POST",
                body);
        LinkedHashMap listMap = objectMapper.readValue(result, LinkedHashMap.class);

        List<LinkedHashMap> linkedHashMaps = extractQueryResult(listMap);

        List<Object> resultValues = new ArrayList<Object>();
        for (LinkedHashMap linkedHashMap: linkedHashMaps){
            resultValues.add(linkedHashMap.get(outputParam));
        }
        resultValues.add(listMap.get("_scroll_id"));
        return resultValues;
    }

    /**
     * 查询接口1：根据一个字段集合查询另一个字段。使用_source作为过滤器。默认返回10条
     * @param indexName 索引名
     * @param outputParam 返回字段，该字段一般用于关联其他索引
     * @param inputParam 查询字段
     * @param inputStr 查询字段must匹配值
     * @return
     * @throws IOException
     */
    public static List<LinkedHashMap> queryFiledByList(String indexName, String outputParam, String inputParam, List<String> inputStr) throws IOException {
        List<LinkedHashMap> listMap = new ArrayList<LinkedHashMap>();
        for (String anInputStr : inputStr) {
            String body = "{\n" +
                    "\t\"_source\":{\n" +
                    "\t\t\"include\":[\"" + outputParam + "\"]\n" +
                    "\t},\n" +
                    "\t\"query\":{\n" +
                    "\t\t\"bool\":{\n" +
                    "\t\t\t\"must\":{\n" +
                    "\t\t\t\t\"match\":{\n" +
                    "\t\t\t\t\t\"" + inputParam + "\":\"" + anInputStr + "\"\n" +
                    "\t\t\t\t}\n" +
                    "\t\t\t}\n" +
                    "\t\t}\n" +
                    "\t}\n" +
                    "}";

        /*根据buildingName查询个人身份证号码*/
            MyURLConnection myURLConnection = new MyURLConnection();
            String result = myURLConnection.request(
                    MyConstParam.esURL + indexName + "/_search",
                    "POST",
                    body);
            LinkedHashMap map = objectMapper.readValue(result, LinkedHashMap.class);

            listMap.addAll(extractQueryResult(map));
        }
        return listMap;
    }

    /**
     * 查询接口1：根据一个字段查询集合。默认返回10条
     * @param indexName 索引名
     * @param inputParam 查询字段
     * @param inputStr 查询字段must匹配值
     * @return
     * @throws IOException
     */
    public static List<LinkedHashMap> queryObjectByField(String indexName, String inputParam, String inputStr) throws IOException {
//        if(size>10000 || size < 1)throw new Exception("size can't be more than 10000, or less than 1\n") ;
        String body =  "{\n" +
                            "\t\"query\":{\n" +
                            "\t\t\"bool\":{\n" +
                            "\t\t\t\"must\":{\n" +
                            "\t\t\t\t\"match\":{\n" +
                            "\t\t\t\t\t\""+inputParam+"\":\""+inputStr+"\"\n" +
                            "\t\t\t\t}\n" +
                            "\t\t\t}\n" +
                            "\t\t}\n" +
                            "\t}\n" +
                            "}";

        /*根据buildingName查询个人身份证号码*/
        MyURLConnection myURLConnection = new MyURLConnection();
        String result= myURLConnection.request(
                MyConstParam.esURL+indexName+"/_search?scroll=1m",
                "POST",
                body);
        LinkedHashMap map = objectMapper.readValue(result, LinkedHashMap.class);
        return extractQueryResult(map);
//        LinkedHashMap linkedHashMap = new LinkedHashMap();
//        linkedHashMap.put("scroll_id",map.get("_scroll_id"));
//        List<LinkedHashMap> linkedHashMaps =  extractQueryResult(map);
//        linkedHashMaps.add(linkedHashMap);
//        return linkedHashMaps;
    }

    /**
     * 查询接口1：根据一个字段查询集合。使用scroll游标查询，最后一个值为scroll_id
     * @param indexName 索引名
     * @param inputParam 查询字段
     * @param inputStr 查询字段must匹配值
     * @param size 返回查询结果的数量
     * @return
     * @throws IOException
     */
    public static List<LinkedHashMap> queryObjectByField(String indexName, String inputParam, String inputStr, int size) throws Exception {
        if(size>10000 || size < 1)throw new Exception("size can't be more than 10000, or less than 1\n") ;
        String body =  "{\n" +
                "\"size\":\""+size+"\"," +
                "\t\"query\":{\n" +
                "\t\t\"bool\":{\n" +
                "\t\t\t\"must\":{\n" +
                "\t\t\t\t\"match\":{\n" +
                "\t\t\t\t\t\""+inputParam+"\":\""+inputStr+"\"\n" +
                "\t\t\t\t}\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        /*根据buildingName查询个人身份证号码*/
        MyURLConnection myURLConnection = new MyURLConnection();
        String result= myURLConnection.request(
                MyConstParam.esURL+indexName+"/_search?scroll=10m",
                "POST",
                body);
        LinkedHashMap map = objectMapper.readValue(result, LinkedHashMap.class);
        LinkedHashMap linkedHashMap = new LinkedHashMap();
        linkedHashMap.put("scroll_id",map.get("_scroll_id"));
        List<LinkedHashMap> linkedHashMaps =  extractQueryResult(map);
        linkedHashMaps.add(linkedHashMap);
        return linkedHashMaps;
    }

    /**
     * 返回一个索引的全部数据 采用scroll游标查询
     * @param indexName
     * @return
     */
    public static List<LinkedHashMap> queryObjectbyIndex(String indexName) throws IOException {
        List<LinkedHashMap> linkedHashMaps = new ArrayList<LinkedHashMap>();
        /*首次查询，每次查询1000条？打开1m接口？*/
        String body =  "{\n" +
                "    \"query\": { \n" +
                "    \t\"match_all\": {}\n" +
                "    },\n" +
                "    \"size\":  10000\n" +
                "}";

        /*根据buildingName查询个人身份证号码*/
        MyURLConnection myURLConnection = new MyURLConnection();
        String result= myURLConnection.request(
                MyConstParam.esURL+indexName+"/_search?scroll=1m",
                "POST",
                body);
        LinkedHashMap map = objectMapper.readValue(result, LinkedHashMap.class);
        List<LinkedHashMap> temp =extractQueryResult(map);
        linkedHashMaps.addAll(temp);
        /*移动游标，直到取完所有数据*/
        while(true){
            /*获取到游标*/
            String scroll_id = (String) map.get("_scroll_id");
            body="{\n" +
                    "\t\"scroll\": \"1m\", \n" +
                    "    \"scroll_id\" : \""+scroll_id+"\"\n" +
                    "}";
            myURLConnection = new MyURLConnection();
            result= myURLConnection.request(
                    MyConstParam.esURL+"_search/scroll",
                    "POST",
                    body);
            map = objectMapper.readValue(result, LinkedHashMap.class);
            temp = extractQueryResult(map);
            if (temp.size()<=0)break;
            linkedHashMaps.addAll(temp);
        }
        return linkedHashMaps;
    }



    /**
     * 使用scroll游标进行查询
     * @param scroll_id
     * @return
     * @throws IOException
     */
    public static List<LinkedHashMap> scrollSearch(String scroll_id) throws IOException {
        String body="{\n" +
                "\t\"scroll\": \"1m\", \n" +
                "    \"scroll_id\" : \""+scroll_id+"\"\n" +
                "}";
        MyURLConnection myURLConnection = new MyURLConnection();
        String result= myURLConnection.request(
                MyConstParam.esURL+"_search/scroll",
                "POST",
                body);
        LinkedHashMap map = objectMapper.readValue(result, LinkedHashMap.class);

        List<LinkedHashMap> temp = extractQueryResult(map);
        LinkedHashMap linkedHashMap = new LinkedHashMap();
        /*放入scroll_id*/
        linkedHashMap.put("scroll_id",map.get("_scroll_id"));
        temp.add(linkedHashMap);
        return temp;
    }

    /**
     * 联表查询,返回一对多关系 TODO 测试该接口
     * @param firstIndexName 第一个索引的名称
     * @param firstFiledName 第一个索引的关联字段
     * @param lastIndexName 第二个索引的名称
     * @param lastFiledName 第二个索引的关联字段
     * @return
     */
    public static List<List<LinkedHashMap>> coupletQuery(String firstIndexName, String firstFiledName, String lastIndexName,String lastFiledName) throws IOException {
        List<List<LinkedHashMap>> lists = new ArrayList<List<LinkedHashMap>>();

        List<LinkedHashMap> hashMapList = queryObjectbyIndex(firstIndexName);
        for(int i = 0; i < hashMapList.size(); ++i){
            List<LinkedHashMap> linkedHashMaps = queryObjectByField(lastIndexName,lastFiledName, (String) hashMapList.get(i).get(firstFiledName));
            if(linkedHashMaps.size()>0) {
                lists.add(linkedHashMaps);
            }
        }

        return lists;
    }

    /**
     * 根据点的经纬度搜索一个维度的相关点
     * @param location 坐标
     * @param indexName 索引名
     * @param distance 距离 500m 1km
     * @return
     */
    public static List<LinkedHashMap> queryNearbyPoint(Map location,String indexName,String distance) throws IOException {

        String body="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"geo_distance\": {\n" +
                "          \"distance\":      \""+distance+"\",\n" +
                "          \"location\": {\n" +
                "            \"lat\":  "+location.get("lat")+",\n" +
                "            \"lon\": "+location.get("lon")+"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MyURLConnection myURLConnection = new MyURLConnection();
        myURLConnection.request(MyConstParam.esURL+indexName+"/_search","POST",body);
        String result= myURLConnection.request(
                MyConstParam.esURL+indexName+"/_search",
                "POST",
                body);
        LinkedHashMap map = objectMapper.readValue(result, LinkedHashMap.class);
        return extractQueryResult(map);
    }

    /**
     * 查询结果中提取属性，撇掉元数据等其他杂乱数据
     * @param queryResult es查询返回结果
     * @return
     */
    public static List<LinkedHashMap> extractQueryResult(HashMap queryResult){
        List<LinkedHashMap> resultList = new ArrayList<LinkedHashMap>();
        List list = (List)((LinkedHashMap) queryResult.get("hits")).get("hits");//获取所有返回值
        for(int i = 0; i < list.size(); ++i){
            LinkedHashMap map1 = (LinkedHashMap) ((LinkedHashMap)list.get(i)).get("_source");//获取每条数据的属性字段
            resultList.add(map1);
        }
        return resultList;
    }

    /**
     * List<Map> 转 List<String>
     * @param input
     * @return
     */
    public static List<String> listMap2ListString(List<LinkedHashMap> input){
        List<String> output = new ArrayList<String>();
        for (Map map: input){
            for(String str: (Set<String>)map.entrySet()){
                output.add(str);
            }
        }
        return output;
    }

    /**
     * 移除通用（无用）属性,好像没啥用，因为不是中文，也要人工去组织。
     * @param queryResult
     * @return
     */
    public static List<Map> removeAttributes(Map queryResult){
        List<Map> resultList = new ArrayList<Map>();
        List list = (List)((LinkedHashMap) queryResult.get("hits")).get("hits");//获取所有返回值
        for(int i = 0; i < list.size(); ++i){
            Map map1 = (Map) ((LinkedHashMap)list.get(i)).get("_source");//获取每条数据的属性字段
            resultList.add(map1);
        }
        return resultList;
    }

    /**
     * 去掉无意义的字段
     * @param input
     * @return
     */
    public static void removeNoMeanField(List<Map> input){
        for (Map map: input){

            map.remove("uuid");map.remove("id");
            map.remove("reserved1");map.remove("reserved2");
            map.remove("reserved3");map.remove("reserved4");
            map.remove("reserved5");map.remove("update_status");
            map.remove("update_time");map.remove("create_time");

        }
        return ;
    }

    /**
     * 去掉无意义的字段
     * @param input
     * @return
     */
    public static void removeNoMeanField(Map input){


        input.remove("uuid");input.remove("id");
        input.remove("reserved1");input.remove("reserved2");
        input.remove("reserved3");input.remove("reserved4");
        input.remove("reserved5");input.remove("update_status");
        input.remove("update_time");input.remove("create_time");


        return;
    }

    /**
     * 数据字段工具，将字段名称转换为中文名称
     * 配置文件实时加载，修改立马生效
     * @param colName
     * @return
     */
    public static String dataDictionary(String dbName,String tbName,String colName) throws IOException {
        InputStream inputStream =ESOperate.class.getResourceAsStream("/dataDictionary.properties");
        Properties properties = new Properties();
        properties.load(new InputStreamReader(inputStream,"UTF-8"));

        return properties.getProperty(dbName+"."+tbName+"."+colName);


//        Properties properties = PropertiesLoaderUtils.loadAllProperties("dataDictionary.properties");
//        return properties.getProperty(dbName+"."+tbName+"."+colName);
    }
}
