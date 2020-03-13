package com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class JSONConsumerUtils {

    public static final Logger logger = LoggerFactory.getLogger(JSONConsumerUtils.class);

    public static void main(String[] args) {
        List<JSONObject> JSONDatas = getJSONDatas("http://opendata.sz.gov.cn/api/1564501785/1/service.xhtml",
                "1","1000","552c36992d9c4e18a35af1bdad5c2535");
        System.out.println(JSONDatas.size());
//        List<JSONObject> JSONDatas = getJSONDatas("http://opendata.sz.gov.cn/api/29200_01503672/1/service.xhtml",
//                "1","100","552c36992d9c4e18a35af1bdad5c2535");
//        System.out.println(JSONDatas.size());
    }

    public static List<JSONObject> getJSONDatas(String url, String page, String rows, String appKey) {

        List<JSONObject> jsonDatas = new ArrayList<>();
        List<JSONObject> jsonDatasNew = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(true);
        logger.info("==>> 开始获取JSON数据...");
        long startTime = System.currentTimeMillis();
//        RequestConfig rc = RequestConfig.custom().setSocketTimeout(5000)
//                .setConnectTimeout(5000).build();
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();

        String resultString = "";
        CloseableHttpResponse response = null;
        if(!StringUtils.isEmpty(url) && !StringUtils.isEmpty(page) && !StringUtils.isEmpty(rows) && !StringUtils.isEmpty(appKey)){
            try {
                // 创建uri
                URIBuilder builder = new URIBuilder(url);
                builder.addParameter("page",page);
                builder.addParameter("rows",rows);
                builder.addParameter("appKey",appKey);

                URI uri = builder.build();
                // 创建http GET请求
                HttpGet httpGet = new HttpGet(uri);
//                // 将配置好请求信息附加到http请求中
//                httpGet.setConfig(rc);

                int retryCount = 5 ;
                while(retryCount > 0){
                    // 执行请求
                    response = httpclient.execute(httpGet);
                    if(response.getStatusLine().getStatusCode() == 200){
                        break;
                    }
                    retryCount--;
                }

                logger.info("statusCode ==>> "+response.getStatusLine().getStatusCode());
                // 判断返回状态是否为200
                if (response.getStatusLine().getStatusCode() == 200) {
//                    logger.info("ContentLength ==>> "+response.getEntity().getContentLength());

                    long startT = System.currentTimeMillis();
                    resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
                    jsonObject = JSON.parseObject(resultString);
                    long endT = System.currentTimeMillis();
                    String costTime = (endT - startT)/1000 +"s" ;
                    logger.info("==>> EntityUtils.toString数据耗时 : "+costTime);

                    jsonDatas = (List<JSONObject>) jsonObject.get("data");
                    logger.info("totalDatas ==> "+jsonObject.get("total"));
                    for (JSONObject object : jsonDatas) {
                        String str = object.toJSONString(object,SerializerFeature.MapSortField);
//                        System.out.println("1==>"+str);
                        object = JSONObject.parseObject(str,Feature.OrderedField);
//                        System.out.println("2==>"+object.toJSONString());
                        jsonDatasNew.add(object);
                    }
                    logger.info("==>> JSON数据获取成功");
                }else{
                    logger.info("JSON数据获取失败，请重试");
                }
            } catch (Exception e) {
                logger.info("JSON数据获取失败，请重试");
                e.printStackTrace();
            } finally {
                try {
                    if (response != null) {
                        response.close();
                    }
                    httpclient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        long endTime = System.currentTimeMillis();
        String costTime = (endTime - startTime)/1000 +"s" ;
        logger.info("==>> 获取数据耗时 : "+costTime);
        return jsonDatasNew ;
    }
}
