package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAO {
    private String namespace;
    private String tableName;
    private int regions;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Configuration conf;
    private Connection connection;
    private HTable table;

    private List<Put> cacheList = new ArrayList<>();

    static{
        conf = HBaseConfiguration.create();
    }
    public HBaseDAO(){
        namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
        tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));

        try {
            if(HBaseUtil.isExistTable(conf, tableName)){
                HBaseUtil.initNameSpace(conf, namespace);
                HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void put(String data){
        try {
            String[] splitOri = data.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];
            String flag = "1";

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTS = String.valueOf(sdf1.parse(buildTime).getTime());

            //散列得分区号
            String  regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);
            //RowKey
            String rowKey = HBaseUtil.genRowKey(regionCode, caller, buildTime, callee, "1", duration);

            //向表中插数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTimeReplace"), Bytes.toBytes(buildTimeReplace));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTS));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes(flag));

            //缓存批量提交
            cacheList.add(put);
            if(cacheList.size()>=30){
                table.put(cacheList);
                table.flushCommits();

                table.close();
                cacheList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
