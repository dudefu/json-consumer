package com.xinyi.xinfo.utils;

import com.xinyi.xinfo.consumer.BatchInsertGP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.Future;


@Component
public class SyncJsonHandler {


    private static final Logger LOG = LoggerFactory.getLogger(SyncJsonHandler.class);

    /**
     * syncMargePsr:(多线程同步处理数据方法). <br/>
     * @param url
     * @param page
     * @param rows
     * @param appKey
     * @param targetTableName
     * @return
     * @since JDK 1.8
     */
    @Async
    public Future<String> syncMargePsr(String url ,String page,String rows,String appKey, String targetTableName,Integer totalRows) {


        LOG.info(String.format("url为:%s ,page为:%s ,rows为:%s ,appKey为:%s ,targetTableName为:%s", url, page,rows,appKey,targetTableName));
        //声明future对象
        Future<String> result = new AsyncResult<String>("");
        BatchInsertGP batchInsertGP = new BatchInsertGP();
        //循环遍历该段旅客集合
        if ( null != rows ) {
            try {
                //数据入库操作
                batchInsertGP.batchInsert(url, page,rows,appKey,targetTableName,totalRows);
                result = new AsyncResult<String>("success,time=" + DateUtil.formatDateTime(new Date()) + ",thread id=" + Thread.currentThread().getName() + ",pageIndex=" + page);
            } catch (Exception e) {
                //记录出现异常的时间，线程name
                result = new AsyncResult<String>("fail,time=" + DateUtil.formatDateTime(new Date()) + ",thread id=" + Thread.currentThread().getName() + ",pageIndex=" + page);
            }
        }
        return result;
    }


}


