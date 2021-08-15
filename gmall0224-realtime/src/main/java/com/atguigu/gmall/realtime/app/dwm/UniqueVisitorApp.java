package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @author: wxw
 * @date: 2021/8/12
 * @desc: 独立访客计算
 */
public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
    // TODO: 2021/8/12  基本环境准备
    //流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
    // TODO: 2021/8/12 检查点相关设置
        /*
    //开启检查点
        //5秒做一次检查点  检查点的精准一次性
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
    //设置检查点超时时间 一分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
    //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
    //设置job取消后,检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    //设置状态后端 内存 文件系统 rocksDB
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/ck/gmall"));
    //指定操作hdfs的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
         */
    // TODO: 2021/8/12 kafka中读取数据
    //声明消费主题以及消费者组
        String topic = "dwd_page_log";
        String groupId = "unique_visitor_app_group";
    //获取kafka消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //读取数据封装流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        // TODO: 2021/8/12 对读取的数据进行类型转换 String->JSONObject
        //jsonObjDS.print(">>>>");
        // TODO: 2021/8/13 按照设备id对设备进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // TODO: 2021/8/13 过滤实现
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {
            //声明状态变量,用于存放上次访问日期
            private ValueState<String> lastVisitedState;
            //转换日期格式工具类
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitedState", String.class);
                //注意 设置日期失效时间 uv可以延伸为日活统计, 如果是日活的话,状态值主要筛选当天是否访问过,
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //默认值 当状态创建或者写入的时候会更新失效时间
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //默认值 状态过期后,如果还没有被清理 是否返回给状态的调用者
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitedState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                //如果是从其他页面跳转过来直接过滤掉
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                //获取状态找那个的上次访问日志
                String lastVisitDate = lastVisitedState.value();
                String curVisitDate = sdf.format(jsonObj.getLong("ts"));
                //System.out.println("当前时间是"+curVisitDate);
                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(curVisitDate)) {
                    //已经访问过
                    return false;
                } else {
                    //还没有访问过 返回true 留下
                    lastVisitedState.update(curVisitDate);
                    return true;
                }

            }
        });
        filterDS.print(">>>>");
        // TODO: 2021/8/13  将过滤后uv数据,写回到kafka的dwm层
        filterDS.map(jsonObj->jsonObj.toJSONString()).addSink(
                MyKafkaUtil.getKafkaSink("dwm_unique_visitor")
        );
        env.execute();
}
}