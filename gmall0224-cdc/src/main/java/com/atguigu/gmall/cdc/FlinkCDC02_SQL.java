package com.atguigu.gmall.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: wxw
 * @date: 2021/8/9
 * @desc: 通过flinkCDC 动态读取mysql 表中数据-- dataStreamAPI
 */
public class FlinkCDC02_SQL {
    public static void main(String[] args) throws Exception {
        // TODO: 2021/8/9 基本环境准备
        //流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置并行度
        env.setParallelism(1);
        // TODO: 2021/8/9 转换为动态表
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT NOT NULL," +
                " name STRING," +
                " age INT" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'hadoop102'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '000000'," +
                " 'database-name' = 'gmall0224_realtime'," +
                " 'table-name' = 't_user'" +
                ")");

        tableEnv.executeSql("select * from user_info").print();

        env.execute();

    }
}
