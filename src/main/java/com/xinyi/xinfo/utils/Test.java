package com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.frameworkset.common.poolman.SQLExecutor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Test {

    public static void main(String[] args) {

//        Object[] objs = {1,2,3,4,5,6};
        String str = "一般经营范围：文化活动策划；会议策划；教育咨询；投资兴办实业（具体项目另行申报）；企业管理咨询、电子商务；国内贸易（不含专营、专卖、专控商品）；经营进出口业务（法律、行政法规、国务院决定禁止的项目除外，限制的项目须取得许可后方可经营）；教育咨询服务；营养健康咨询服务；健康管理咨询服务（须经审核的诊疗活动、心理咨询除外，不含许可经营项目，法律法规禁止经营的项目不得经营）；亚健康调理（不含医疗服务，法律法规禁止经营的项目不得经营）；医疗用品及器材零售（不含药品及医疗器械）；非许可类医疗器械经营；健康科学项目研究成果推广；针灸医学的研究；医疗技术推广服务；护理服务（不涉及提供住宿、医疗诊断、治疗及康复服务）；保健按摩。；许可经营范围：保健食品、预包装食品的销售。";
        String str2 = str.replace(" ","");
        System.out.println(str.length());
    }

    public static String writeFile(List<JSONObject> list){
        FileWriter out = null;
        String filePath = "./"+ UUID.randomUUID();

        try{
            out = new FileWriter(new File(filePath));
            List<String> fields = null ;
            for(Map.Entry<String,Object> entry:list.get(0).entrySet()){
                fields.add(entry.getKey());
            }
            for(int i=0;i<list.size();i++){
                int count = 0 ;
                for(Map.Entry<String,Object> entry:list.get(i).entrySet()){
                    count++;
                    if(fields.contains(entry.getKey())){
                        if(entry.getValue() == null){
                            out.write("null");
                        }else{
                            String str = String.valueOf(entry.getValue()).replace("\n","") ;
                            out.write(str);
                        }
                        if(count != list.get(i).size() - 1){
                            out.write("^");
                        }
                    }else{
                        out.write("null");
                        if(count != list.get(i).size() - 1){
                            out.write("^");
                        }
                    }
                }
                if(i != list.size() - 1){
                    out.write("\n");
                }
//                Object[] objs = list.get(i).values().toArray();
//                for(int j=0;j<objs.length;j++){
//                    if(objs[j] == null){
//                        out.write("null");
//                    }else{
//                        String str = String.valueOf(objs[j]).replace("\n","") ;
//                        out.write(str);
//                    }
//                    if(j != objs.length - 1){
//                        out.write("^");
//                    }
//                }
//                if(i != list.size() - 1){
//                    out.write("\n");
//                }
            }
            out.flush();
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(out != null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return filePath;
    }

    public static Object[] ArrayCopy(Object[] objs,List<String> fields){

        Object[] objsTemp = null ;
        if(objs.length != fields.size()){
            if(objs.length < fields.size()){
                objsTemp = new Object[fields.size()];
                System.arraycopy(objs,0,objsTemp,0,objs.length);
            }else if(objs.length > fields.size()){
                objsTemp = new Object[objs.length];
                System.arraycopy(fields,0,objsTemp,0,fields.size());
            }
        }
        return objsTemp;
    }

    public void test(){
        List<Integer> datas = new ArrayList<>();
//        for (int i = 0; i < 13; i++) {
//            datas.add(i);
//        }
//        logger.info(JSON.toJSONString(datas));
//        logger.info("总数据条数："+datas.size());
//
//        int totalSize = datas.size();
//        int batchSize = 5 ;
//        int startIndex = 0 ;
//        int endIndex = batchSize ;
//        List<Integer> datasNew = null ;
//        while (totalSize >= batchSize ){
//            datasNew = datas.subList(startIndex,endIndex);
//            logger.info("==> 开始往GP数据库插入json数据");
//            logger.info(JSON.toJSONString(datasNew));
//            logger.info("==> 已插入数据条数 ："+batchSize);
//            //剩余数据条数
//            totalSize = totalSize-batchSize ;
//            logger.info("==> 剩余数据条数 : " + totalSize);
//            startIndex = startIndex + batchSize;
//            logger.info("startIndex ==> " + startIndex);
//            if(totalSize >= batchSize ) {
//                endIndex = endIndex + batchSize;
//            }else{
//                endIndex = endIndex + totalSize;
//            }
//            logger.info("endIndex ==> " + endIndex);
//        }
//        if(totalSize != 0 ){
//            //List<Map> datas = JSONConsumerUtils.getJSONDatas(url, page, String.valueOf(totalSize), appKey);
//            List<Integer> endDatas = datas.subList(startIndex,endIndex);
//            totalSize = endDatas.size() ;
//            logger.info("==> 剩余数据条数 : " + totalSize);
//            logger.info("==> 开始往GP数据库插入json数据");
//            logger.info(JSON.toJSONString(endDatas));
//            logger.info("==> 已插入数据条数 ："+totalSize);
//            logger.info("==> 已插入全部数据.");
//        }
    }
}
