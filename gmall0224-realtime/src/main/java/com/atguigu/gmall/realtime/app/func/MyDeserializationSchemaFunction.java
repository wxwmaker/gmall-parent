package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author: wxw
 * @date: 2021/8/10
 * @desc: 自定义反序列化器
 */
public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStrut = valueStruct.getStruct("source");
        //获取数据库的名称
        String database = sourceStrut.getString("db");
        //获取表名
        String table = sourceStrut.getString("table");

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
        //目标格式：{"id":74603,"order_id":28641,"order_status":"1005","operate_time":"2021-07-30 11:35:49"}}
        Struct afterStruct = valueStruct.getStruct("after");
        JSONObject dataJsonObj = new JSONObject();
        if(afterStruct!=null){
            for (Field field : afterStruct.schema().fields()) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(field);
                dataJsonObj.put(fieldName,fieldValue);
            }
        }
        jsonObj.put("data",dataJsonObj);

        //向下游传递数据
        collector.collect(jsonObj.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
