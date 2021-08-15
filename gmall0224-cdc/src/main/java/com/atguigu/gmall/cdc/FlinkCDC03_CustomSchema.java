package com.atguigu.gmall.cdc;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
/**
 * @author: wxw
 * @date: 2021/8/9
 * @desc: 通过flinkcdc 动态读取mysql 表中数据 --DataStreamAPI
 *
 * 自定义反序列化器
 */
public class FlinkCDC03_CustomSchema {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        ///TODO 2.开启检查点   Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,
        // 需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置超时时间为1分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //2.3 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000L));
        //2.4 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/flinkCDC"));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        */


        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall0224_realtime")
                .tableList("gmall0224_realtime.t_user")
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MySchema())
                .build();


        env
                .addSource(sourceFunction)
                .print();

        env.execute();
    }
}


class MySchema implements DebeziumDeserializationSchema<String> {
    /*
       ConnectRecord
       {
           value=Struct{
               after=Struct{id=1,name=liuxintong,age=17},
               source=Struct{
                   db=gmall0224_realtime,
                   table=t_user
               },
               op=c
           },
       }*/
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStruct = valueStruct.getStruct("source");

        //获取数据库的名称
        String database = sourceStruct.getString("db");
        //获取表名
        String table = sourceStruct.getString("table");
        //获取类型
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if(type.equals("create")){
            type="insert";
        }
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("database",database);
        jsonObj.put("table",table);
        jsonObj.put("type",type);

        //获取影响的数据data
        // 源格式：id=1,name=liuxintong,age=17
        //目标格式{"id":74603,"order_id":28641,"order_status":"1005","operate_time":"2021-07-30 11:35:49"}}
        Struct afterStruct = valueStruct.getStruct("after");
        JSONObject dataObj = new JSONObject();
        if (afterStruct!=null) {
            for (Field field : afterStruct.schema().fields()) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(field);
                dataObj.put(fieldName, fieldValue);
            }
        }
        jsonObj.put("data", dataObj);
        //向下游传递数据
        collector.collect(jsonObj.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}