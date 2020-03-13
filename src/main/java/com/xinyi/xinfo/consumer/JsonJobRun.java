package com.xinyi.xinfo.consumer;

import com.xinyi.xinfo.utils.DateUtil;
import com.xinyi.xinfo.utils.SyncJsonHandler;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class JsonJobRun {

    private static final Logger LOG = LoggerFactory.getLogger(JsonJobRun.class);

    @Autowired
    private SyncJsonHandler syncJsonHandler;

    //核心线程数
    @Value("${json.core.poolsize}")
    private int threadSum;

    private static PropertiesContainer propertiesContainer = new PropertiesContainer();
    static {
        propertiesContainer.addConfigPropertiesFile("application.properties");
    }

    public void JsonJobRunTest(String url, String totalRows, String appKey, String targetTableName ) throws SQLException {
        //入库开始时间
        Long inserOrUpdateBegin = System.currentTimeMillis();
        LOG.info("数据更新开始时间:"+ DateUtil.formatDateTime(new Date()));

        //接收集合各段的 执行的返回结果
        List<Future<String>> futureList = new ArrayList<Future<String>>();
        Integer totalPage = 1;
        Integer batchSize = Integer.valueOf(propertiesContainer.getProperty("batchSize"));
        Integer totalSize = Integer.valueOf(totalRows);
        Integer totalDataRows = Integer.valueOf(totalRows);

        //集合总条数
        if(totalRows != null){
            if (totalSize > batchSize) {
                totalPage = totalSize / batchSize + (totalSize % batchSize > 0 ? 1 : 0);
            }

            int listStart,listEnd;
            //当总条数不足threadSum条时 用总条数 当做线程切分值
            if(threadSum > totalPage){
                threadSum = totalPage;
                for (int i = 1; i <= threadSum ; i++) {
                    if(totalSize >= batchSize ){
                        totalSize -= batchSize ;
                        //每段数据集合并行入库
                        futureList.add(syncJsonHandler.syncMargePsr(url,i+"",batchSize+"",appKey,targetTableName,totalDataRows));
                    }else{
                        futureList.add(syncJsonHandler.syncMargePsr(url,i+"",totalSize+"",appKey,targetTableName,totalDataRows));
                    }
                }
            }else{
                for (int i = 1; i <= threadSum ; i++) {
                    if(totalSize >= batchSize ){
                        totalSize -= batchSize ;
                        //每段数据集合并行入库
                        futureList.add(syncJsonHandler.syncMargePsr(url,i+"",batchSize+"",appKey,targetTableName,totalDataRows));
                    }else{
                        futureList.add(syncJsonHandler.syncMargePsr(url,i+"",totalSize+"",appKey,targetTableName,totalDataRows));
                    }
                }
            }


            //对各个线程段结果进行解析
            for(Future<String> future : futureList){
                String str ;
                if(null != future ){
                    try {
                        str = future.get().toString();
                        LOG.info("current thread id ="+Thread.currentThread().getName()+",result="+str);
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.info("线程运行异常！");
                    }
                }else{
                    LOG.info("线程运行异常！");
                }

            }
        }
        Long inserOrUpdateEnd = System.currentTimeMillis();
        LOG.info("数据更新结束时间:"+DateUtil.formatDateTime(new Date())+"。此次更新数据花费时间为："+((inserOrUpdateEnd-inserOrUpdateBegin)/1000)+" s");

    }

}
