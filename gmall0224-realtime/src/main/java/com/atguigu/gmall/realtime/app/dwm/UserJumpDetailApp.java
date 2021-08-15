package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author: wxw
 * @date: 2021/8/13
 * @desc: 用户跳出明细统计
 */
public class UserJumpDetailApp {
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
        String groupId = "user_jump_detail_app_group";
        //获取kafka消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //读取数据封装流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //jsonObjDS.print(">>>>");
        // TODO: 2021/8/13 指定watermark以及提取时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                //生成策略 单调递增 还有个乱序
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        //提取事件时间字段
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                                        return jsonObject.getLong("ts");
                                    }
                                }

                        )
        );
        //cep模式匹配
        // TODO: 2021/8/13 按照mid分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 7.定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                //条件1：开启一个新的会话访问(第一条数据) 第一条数据后再第二条数据
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("second").where(
                //条件2：访问了网站的其它页面(访问了当前页面) 不写if直接返回true也行 第二条数据直接过 第二条数据后接着第三条数据直接过
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.seconds(10)); //没通过的都放到侧输出流中

        //TODO 8.将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);
        //TODO 9.从流中提取数据
        env.execute();


    }
}
