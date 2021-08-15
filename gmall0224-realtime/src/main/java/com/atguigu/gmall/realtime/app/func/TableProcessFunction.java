package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author: wxw
 * @date: 2021/8/10
 * @desc: 动态分流实现
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //声明维度侧输出流的标签
    private OutputTag<JSONObject> dimTag;
    //声明广播状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //声明连接对象
    private Connection conn;

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    //处理业务流中数据, maxwell 从业务数据库仲裁及到的数据
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        //注意：我们在使用Maxwell处理历史数据的时候，类型是bootstrap-insert  我们这里修复为insert
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }
        //拼接key
        String key = table + ":" + type;
        //获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //从状态中获取配置信息
        TableProcess tableProcess = broadcastState.get(key);

        if(tableProcess != null){
            //在配置表中找到了该操作对应的配置  判断是事实数据还是维度数据
            String sinkTable = tableProcess.getSinkTable();
            jsonObj.put("sink_table",sinkTable);

            //在向下游传递数据之前,将不需要的字段过滤掉 过滤思路:从配置表中读取保留字段,根据保留字段,对data中的属性进行过滤
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            filterColumns(dataJsonObj,tableProcess.getSinkColumns());



            String sinkType = tableProcess.getSinkType();
            if(sinkType.equals(TableProcess.SINK_TYPE_HBASE)){
                //维度数据   放到维度侧输出流汇总
                ctx.output(dimTag,jsonObj);
            }else if(sinkType.equals(TableProcess.SINK_TYPE_KAFKA)){
                //事实数据  放到主流中
                out.collect(jsonObj);
            }
        }else{
            //在配置表中没有该操作对应的配置
            System.out.println("No this Key in TableProcess:" + key);
        }

    }
    //过滤字段
    private void filterColumns(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        //Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
        //for (;it.hasNext();) {
        //    if(!columnList.contains(it.next().getKey())){
        //        it.remove();
        //    }
        //}
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));
    }

    //处理广播流中的数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> collector) throws Exception {
        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //将json格式字符串转化为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取配置表中一条配置信息
        ;
        TableProcess tableProcess = JSON.parseObject(jsonObj.getString("data"), TableProcess.class);
        //获取列数据 也就是业务数据库的表名
        String sourceTable = tableProcess.getSourceTable();
        //操作类型
        String operateType = tableProcess.getOperateType();
        //数据类型    hbase--维度     kafka---事实
        String sinkType = tableProcess.getSinkType();
        //指定输出目的地
        String sinkTable = tableProcess.getSinkTable();
        //主键
        String sinkPk = tableProcess.getSinkPk();
        //指定保留字段
        String sinkColumns = tableProcess.getSinkColumns();
        //指定建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();
        //如果说读取到的配置信息是维度配置的话,那么提前将维度表创建出来
        // sinkExtend 扩展
        if (sinkType.equals(TableProcess.SINK_TYPE_HBASE)&&"insert".equals(operateType)){
            checkTable(sinkTable,sinkPk,sinkColumns,sinkExtend);
        }

        //拼接key
        String key = sourceTable + ":" + operateType;
        // 将配置信息放到状态中
        broadcastState.put(key,tableProcess);
    }
    //在处理配置数据的时候  提前简历维度表 create table if not exist 表空间.表名(字段名 数据类型);
    private void checkTable(String tableName, String pk, String fields, String ext) throws SQLException {
        //对主键进行非空处理
        if(pk==null){
            pk = "id";
        }
        //对建表扩展进行空值处理
        if (ext==null){
            ext = "";
        }
        StringBuilder createSql = new StringBuilder("create table if not exists "+ GmallConfig.HBASE_SCHEMA+"."+tableName+"(");

        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            //判断是否为主键
            if(field.equals(pk)){
                createSql.append(field + " varchar primary key ");
            }else{
                createSql.append(field + " varchar ");
            }
            if(i < fieldsArr.length - 1){
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);
        System.out.println("Phoenix中的建表语句：" + createSql);

        PreparedStatement ps = null;
        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在Phoenix中建表失败");
        }finally {
            //释放资源
            if(ps != null){
                ps.close();
            }
        }

    }
}


