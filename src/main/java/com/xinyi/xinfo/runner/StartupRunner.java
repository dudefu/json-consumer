package com.xinyi.xinfo.runner;

import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.xinyi.xinfo.consumer.BatchInsertGP;
import com.xinyi.xinfo.consumer.JsonJobRun;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;

import java.sql.SQLException;

@Order(1)
public class StartupRunner implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(StartupRunner.class);

    @Autowired
    private JsonJobRun jsonJobRun;

    private static PropertiesContainer propertiesContainer = new PropertiesContainer();
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

    @Override
    public void run(String... args) throws SQLException {
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

            //创建数据库连接池
            buildDBConfigAndStartDatasource();

            //清空数据
            SQLExecutor.delete("delete from " + targetTableName);

            //清空数据
            jsonJobRun.JsonJobRunTest(url,totalRow,appKey,targetTableName);

        }
        System.exit(0);
    }
}
