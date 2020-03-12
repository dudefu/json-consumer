package com.xinyi.xinfo.runner;

import com.xinyi.xinfo.consumer.BatchInsertGP;
import com.xinyi.xinfo.utils.GreenPlumUtils;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Order(1)
public class StartupRunner implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(StartupRunner.class);

    @Autowired
    private JsonJobRun jsonJobRun;

    private static PropertiesContainer propertiesContainer = new PropertiesContainer();
    static {
        propertiesContainer.addConfigPropertiesFile("application.properties");
    }
    //线程组
    static ExecutorService pool = Executors.
            newCachedThreadPool();

    @Override
    public void run(String... args){
        /**
         * args[0] --- url
         * args[2] --- totalRows
         * args[3] --- appKey
         * args[4] --- targetTableName
         */
        if(args.length == 0){
            logger.warn("main方法缺少运行参数（url,totalRows,indexName,targetTableName）");
            System.exit(0);
        }else{
            String url = args[0];
            String totalRow = args[1];
            String appKey = args[2];
            String targetTableName = args[3];
            BatchInsertGP batchInsertGP = new BatchInsertGP();

            Integer batchSize = Integer.valueOf(propertiesContainer.getProperty("batchSize"));
            Integer page = 1;

//            if (totalRow > batchSize) {
//                page = totalRow / batchSize + (totalRow % batchSize > 0 ? 1 : 0);
//            }

            //batchInsertGP.batchInsert(url, "1","1000",appKey,targetTableName);

//            JsonJobRun jsonJobRun = new JsonJobRun() ;
            jsonJobRun.JsonJobRunTest(url,totalRow,appKey,targetTableName);
//            for (int i = 1; i <= page; i++) {
//                if(totalRow >= batchSize ){
//                    totalRow -= batchSize ;
//                    int finalI = i;
//                    pool.execute(() -> {
//                        batchInsertGP.batchInsert(url, finalI,batchSize,appKey,targetTableName);
//                    });
//                }else{
//                    pool.execute(() -> {
//                        batchInsertGP.batchInsert(url,i,batchSize,appKey,targetTableName);
//                    });
//                }
//            }
        }
        System.exit(0);
    }
}
