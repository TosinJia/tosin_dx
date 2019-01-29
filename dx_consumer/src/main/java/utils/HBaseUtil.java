package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author TosinJia
 * 1 NameSpace 命名空间
 * 2 createTable 表
 * 3 isTable 判断表是否存在
 * 4 Region、RowKey、分区键
 */
public class HBaseUtil {

    /**
     * 初始化命名空间
     * @param conf 配置对象
     * @param namespace 命名空间名称
     * @throws IOException
     */
    public static void initNameSpace(Configuration conf, String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // 命名空间描述符
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespace)
                .addConfiguration("AUTHOR", "Tosin")
                .build();
        //通过admin创建命名空间
        admin.createNamespace(nd);
        //关闭资源
        close(connection, admin);
    }

    /**
     * 关闭资源
     * @param connection
     * @param admin
     * @throws IOException
     */
    private static void close(Connection connection, Admin admin) throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    /**
     * 创建HBase表
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //判定表是否存在
        if(isExistTable(conf, tableName)){
            return;
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf: columnFamily) {
            htd.addFamily(new HColumnDescriptor(cf));
        }
        //协处理器
//        htd.addCoprocessor();
        //创建表
        admin.createTable(htd, genSplitKeys(regions));
        //关闭资源
        close(connection, admin);
    }

    /**
     * 分区键
     * @param regions
     * @return
     */
    private static byte[][] genSplitKeys(int regions) {
        //存放分区键的数组
        String[] keys = new String[regions];
        //格式化分区键的形式 00|01|02|
        DecimalFormat df = new DecimalFormat("00");
        for(int i = 0; i<regions; i++){
            keys[i] = df.format(i)+"|";
        }

        byte[][] splitKeys = new byte[regions][];
        //排序 保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(int i = 0; i<regions; i++){
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        //输出
        Iterator<byte[]> iterator = treeSet.iterator();
        int index = 0;
        while(iterator.hasNext()){
            byte[] next = iterator.next();
            splitKeys[index++] = next;
        }
        return splitKeys;
    }

    /**
     * 判定表是否存在
     * @param conf
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        boolean res = admin.tableExists(TableName.valueOf(tableName));
        close(connection, admin);
        return res;
    }


    /**
     * 生产rowkey：regionCode_caller_buildTime_callee_flag_duration
     * @param regionCode 散列的键 防止数据倾斜
     * @param caller 主叫
     * @param buildTime 建立时间
     * @param callee 被叫
     * @param flag 表明主叫还是被叫
     * @param duration 通话持续时长
     * @return 返回rowKey
     */
    public static String genRowKey(String regionCode, String caller, String buildTime, String callee, String flag, String duration){
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode+"_")
                .append(caller+"_")
                .append(buildTime+"_")
                .append(callee+"_")
                .append(flag+"_")
                .append(duration+"_");
        return sb.toString();
    }

    /**
     * 生成分区号
     * @param caller
     * @param buildTime
     * @param regions region个数
     * @return 返回分区号
     */
    public static String genRegionCode(String caller,String buildTime,int regions){
        int len = caller.length();
        //取出主叫后四位
        String lastPhone = caller.substring(len - 4);
        //取出年月
        String ym = buildTime.replaceAll("_", "")
                .substring(0, 6);
        //做离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        //散列操作2
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }
}
