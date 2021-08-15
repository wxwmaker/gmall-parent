package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.app.func.DimSInk;
import com.atguigu.gmall.realtime.app.func.MyDeserializationSchemaFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * @author: wxw
 * @date: 2021/8/8
 * @desc: 业务数据动态分流
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2021/8/8 基本环境准备
        //流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        // TODO: 2021/8/8 检查点设置
        /*
        //开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //设置job取消后,检查点是否保留
       env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置状态后端 内存\文件系统\rocksdb
        env.setStateBackend(new FsStateBackend("xxx"));
        //指定hdfs的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        // TODO: 2021/8/8 从kafka中读取数据
        String topic="ods_base_db_m";
        String groupId="base_db_app_group";
        //获取消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //读取数据 封装流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        // TODO: 2021/8/8 对数据类型进行转换 String->JSONObject
        SingleOutputStreamOperator<JSONObject> josnObjDS = kafkaDS.map(JSON::parseObject);
        // TODO: 2021/8/8 简单的etl
        SingleOutputStreamOperator<JSONObject> filterDS = josnObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        boolean flag = jsonObject.getString("table") != null
                                && jsonObject.getString("table").length() > 0
                                && jsonObject.getJSONObject("data") != null
                                && jsonObject.getString("data").length() > 3;
                        return flag;
                    }
                }
        );

        //filterDS.print(">>>>>");

        // TODO: 2021/8/10 使用flinkCDC 读取配置表数据
        //获取sourceFunction
        SourceFunction<String> mySQLSourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall0224_realtime")
                .tableList("gmall0224_realtime.table_process")
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializationSchemaFunction())
                .build();

        //读取数据封装流
        DataStreamSource<String> mysSQLDS = env.addSource(mySQLSourceFunction);
        MapStateDescriptor<String, TableProcess>
                mapStateDescriptor = new MapStateDescriptor<>("table_process", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mysSQLDS.broadcast(mapStateDescriptor);
        //调用非广播流的connect 方法 将业务流与配置流进行连接
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcast);

        // TODO: 2021/8/8 动态分流 将维度数据放到维度侧输出流  实时数据放到主流
        //声明维度侧输出流标记
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag"){};
        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(
                new TableProcessFunction(dimTag,mapStateDescriptor)
        );
        // 获取维度侧输出流
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);
        realDS.print(">>>");
        dimDS.print("###");
        // TODO: 2021/8/8 将维度侧输出流的数据写到hbase 中
        dimDS.addSink(new DimSInk());
        // TODO: 2021/8/8 将主流数据写回到kafka的dwd层
        realDS.addSink(
                MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long aLong) {
                        String topic = jsonObj.getString("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topic,jsonObj.getJSONObject("data").toJSONString().getBytes());
                    }
                })
        );

        env.execute();
    }
}
