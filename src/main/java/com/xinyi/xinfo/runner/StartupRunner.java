package com.xinyi.xinfo.runner;

import com.xinyi.xinfo.consumer.BatchInsertGP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;

@Order(1)
public class StartupRunner implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(StartupRunner.class);

    @Override
    public void run(String... args){
        /**
         * eg: streaminglog,streaminglog_1574991501027,streaminglog_157479797979797
         * args[0] --- url
         * args[1] --- page
         * args[2] --- rows
         * args[3] --- appKey
         * args[4] --- targetTableName
         */
        if(args.length == 0){
            logger.warn("main方法缺少运行参数（server,port,indexName,targetTableName）");
            System.exit(0);
        }else{
            BatchInsertGP batchInsertGP = new BatchInsertGP();
            batchInsertGP.batchInsert(args[0],args[1],args[2],args[3],args[4]);

        }
    }
}
