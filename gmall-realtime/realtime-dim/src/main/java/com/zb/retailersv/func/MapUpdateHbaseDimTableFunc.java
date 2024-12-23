package com.zb.retailersv.func;

import com.alibaba.fastjson.JSONObject;
import com.zbm.util.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject,JSONObject> {

    private Connection connection;
    private final String hbaseNameSpace;
    private final String zkHostList;
    private HbaseUtils hbaseUtils;

    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils(zkHostList);
        connection = hbaseUtils.getConnection();
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        //取出数据是否是什么类型的字段
        String op = jsonObject.getString("op");
        //判断是否是删除的数据,如果是的话就删除数据
        if ("d".equals(op)){
            //deleteTable使用此方法进行删除
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
        }else if ("r".equals(op) || "c".equals(op)){
            // String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
            if (!hbaseUtils.tableIsExists(hbaseNameSpace+":"+tableName)){
                hbaseUtils.createTable(hbaseNameSpace,tableName);
            }
        }else {
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
            // String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
            hbaseUtils.createTable(hbaseNameSpace,tableName);
        }
        return jsonObject;
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
