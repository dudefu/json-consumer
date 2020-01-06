package com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.commons.lang.StringUtils;
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
        List<JSONObject> JSONDatas = getJSONDatas("http://opendata.sz.gov.cn/api/29200_00403601/1/service.xhtml",
                "1","100","552c36992d9c4e18a35af1bdad5c2535");
    }

    public static List<JSONObject> getJSONDatas(String url, String page, String rows, String appKey) {

        List<JSONObject> jsonDatas = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(true);
        logger.info("==>> 开始获取JSON数据...");
        long startTime = System.currentTimeMillis();
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
                // 执行请求
                response = httpclient.execute(httpGet);
                // 判断返回状态是否为200
                if (response.getStatusLine().getStatusCode() == 200) {
                    resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
//                    logger.info(resultString);
                    jsonObject = JSON.parseObject(resultString,Feature.OrderedField);
                    jsonDatas = (List<JSONObject>) jsonObject.get("data");
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
        return jsonDatas ;
    }
}
