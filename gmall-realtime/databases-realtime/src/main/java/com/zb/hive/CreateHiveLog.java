package com.zb.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CreateHiveLog {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        String createHiveCatalogDDL="create catalog hive_catalog with(" +
                " 'type'='hive'," +
                " 'default-database'='default'," +
                " 'hive-conf-dir'='D:/idea/ideacode/gmall-realtime/databases-realtime/src/main/resources')";

        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "D:/idea/ideacode/gmall-realtime/databases-realtime/src/main/resources");

        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql(createHiveCatalogDDL).print();

    }
}
