package com.xinyi.xinfo.consumer;

import com.alibaba.fastjson.JSONObject;
import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.xinyi.xinfo.utils.CopyDataToGp;
import com.xinyi.xinfo.utils.FieldsOperate;
import com.xinyi.xinfo.utils.GreenPlumUtils;
import com.xinyi.xinfo.utils.JSONConsumerUtils;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

public class BatchInsertGP {

    public static final Logger logger = LoggerFactory.getLogger(BatchInsertGP.class);
    private static PropertiesContainer propertiesContainer = new PropertiesContainer();
    private static final Connection conn = GreenPlumUtils.getConnection();
    //已插入数据条数
    private static int allRecords = 0;

    static {
        propertiesContainer.addConfigPropertiesFile("application.properties");
    }

    protected static void buildDBConfigAndStartDatasource() {

        String dbName = propertiesContainer.getProperty("db.name");
        String dbUser = propertiesContainer.getProperty("db.user");
        String dbPassword = propertiesContainer.getProperty("db.password");
        String dbDriver = propertiesContainer.getProperty("db.driver");
        String dbUrl = propertiesContainer.getProperty("db.url");


        String validateSQL = propertiesContainer.getProperty("db.validateSQL");


        String _jdbcFetchSize = propertiesContainer.getProperty("db.jdbcFetchSize");
        Integer jdbcFetchSize = null;
        if (_jdbcFetchSize != null && !_jdbcFetchSize.equals(""))
            jdbcFetchSize = Integer.parseInt(_jdbcFetchSize);

        //启动数据源
        SQLUtil.startPool(dbName,//数据源名称
                dbDriver,//mysql驱动
                dbUrl,//mysql链接串
                dbUser, dbPassword,//数据库账号和口令
                validateSQL, //数据库连接校验sql
                jdbcFetchSize // jdbcFetchSize
        );


    }

    /**
     * 批量插入数据
     *
     * @param url
     * @param page
     * @param rows
     * @param appKey
     * @param targetTableName
     * @param batchsize
     * @return
     */
    public static boolean batchInsert(String url, String page, String rows, String appKey, String targetTableName, String batchsize) {

        int batchSize = Integer.valueOf(batchsize);
        boolean result = true;
        //已插入数据条数
        int allRecords = 0;
        Map<Integer, JSONObject> map = new HashMap<>();


        //创建数据库连接池
        buildDBConfigAndStartDatasource();
        try {
            //清空数据
//            SQLExecutor.delete("delete from " + targetTableName);

            //分页插入数据
//            final int batchSize = Integer.parseInt(propertiesContainer.getProperty("batchSize"));

            logger.info("==> 拉取数据... ...");
            List<JSONObject> datas = JSONConsumerUtils.getJSONDatas(url, page, rows, appKey);

            //循环遍历每个jsonobject，得到长度的map集合
            for (JSONObject jsonObject : datas) {
                map.put(jsonObject.size(), jsonObject);
            }
            List<Integer> list = new ArrayList<>();
            for (Integer key : map.keySet()) {
                list.add(key);
            }
            //对得到的map集合进行排序
            Collections.sort(list);

            int totalSize = datas.size();
            logger.info("==> 数据总条数totalSize : " + totalSize);
            if (totalSize != 0) {
                long startTime = System.currentTimeMillis();
                List<JSONObject> datasNew = null;

                //获取data字段
                String tableColumns = FieldsOperate.getFields(map.get(list.get(list.size() - 1)), targetTableName).toLowerCase();
                logger.info("===>>> tableColumns : " + tableColumns);

                int startIndex = 0;
                int endIndex = batchSize;
                while (totalSize >= batchSize) {
                    datasNew = datas.subList(startIndex, endIndex);
                    logger.info("==> 开始往GP数据库插入json数据");
                    long copyDataResult = CopyDataToGp.copyData(targetTableName, tableColumns, datasNew, conn);
                    allRecords = allRecords + batchSize;
//                    logger.info("==> 已插入数据条数 ："+allRecords);
                    logger.info("==> 已插入数据条数 ：" + copyDataResult);
                    //按batchSize循环递减
                    totalSize = totalSize - batchSize;
                    startIndex = startIndex + batchSize;
                    logger.info("startIndex ==> " + startIndex);
                    if (totalSize >= batchSize) {
                        endIndex = endIndex + batchSize;
                    } else {
                        endIndex = endIndex + totalSize;
                    }
                    logger.info("endIndex ==> " + endIndex);
                }
                if (totalSize != 0) {
                    long oneStartTime = System.currentTimeMillis();
                    List<JSONObject> endDatas = datas.subList(startIndex, endIndex);
                    totalSize = endDatas.size();
                    logger.info("==> 剩余数据条数 : " + totalSize);
                    logger.info("==> 开始往GP数据库插入json数据");
                    long copyDataResult = CopyDataToGp.copyData(targetTableName, tableColumns, endDatas, conn);
                    allRecords = allRecords + totalSize;
//                    logger.info("==> 已插入数据条数 ："+allRecords);
                    logger.info("==> 已插入数据条数 ：" + copyDataResult);
                    logger.info("==> 已插入全部数据.");
                    long oneEndTime = System.currentTimeMillis();
                    logger.info("===>> 单次插入costTime = " + (oneEndTime - oneStartTime) / 1000 + " s");
                }
                long endTime = System.currentTimeMillis();
                logger.info("==> 数据插入总耗时：" + (endTime - startTime) / 1000 + "s");
            } else {
                result = false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }
        return result;
    }

    /**
     * 一次性插入数据
     *
     * @param url
     * @param page
     * @param rows
     * @param appKey
     * @param targetTableName
     * @return
     */
    public static boolean batchInsert(String url, String page, String rows, String appKey, String targetTableName,Integer totalRows) {

        boolean result = true;
        Map<Integer, JSONObject> map = new HashMap<>();

        //创建数据库连接池
        buildDBConfigAndStartDatasource();
        try {
            //清空数据
//            SQLExecutor.delete("delete from " + targetTableName);

            logger.info("==> 拉取数据... ...");
            List<JSONObject> datas = JSONConsumerUtils.getJSONDatas(url, page, rows, appKey);

            //循环遍历每个jsonobject，得到长度的map集合
            for (JSONObject jsonObject : datas) {
                map.put(jsonObject.size(), jsonObject);
            }
            List<Integer> list = new ArrayList<>();
            for (Integer key : map.keySet()) {
                list.add(key);
            }
            //对得到的map集合进行排序
            Collections.sort(list);

            int totalSize = datas.size();
            logger.info("==> 单次插入数据总条数totalSize : " + totalSize);
            if (totalSize != 0) {
                long startTime = System.currentTimeMillis();
                //获取data字段
                String tableColumns = FieldsOperate.getFields(map.get(list.get(list.size() - 1)), targetTableName).toLowerCase();
                logger.info("===>>> tableColumns : " + tableColumns);
                logger.info("==> 开始往GP数据库插入json数据");
                long copyDataResult = CopyDataToGp.copyData(targetTableName, tableColumns, datas, conn);
                allRecords += copyDataResult;
                logger.info("==> 已插入数据条数 ：" + allRecords);
                logger.info("==> 剩余数据条数 ：" + (totalRows - allRecords));

                long endTime = System.currentTimeMillis();
                logger.info("==> 数据插入总耗时：" + (endTime - startTime) / 1000 + "s");
            } else {
                result = false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }
        return result;
    }
}
