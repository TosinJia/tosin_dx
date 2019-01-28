package utils;

import java.text.DecimalFormat;

public class HBaseUtil {

    /**
     * 生产rowkey：regionCode_caller_buildTime_callee_flag_duration
     * @param regionCode 防止数据倾斜
     * @param caller
     * @param buildTime
     * @param callee
     * @param flag
     * @param duration
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
     *
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
        //做离散操作
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;

        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }
}
