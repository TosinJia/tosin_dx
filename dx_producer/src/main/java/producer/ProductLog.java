package producer;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: TosinJia
 * @date: 2019/1/27 10:32
 * @description:
 *
 * java -cp .jar ProductLog filepath
 */
public class ProductLog {
    private String startTime = "2019-01-01";
    private String endTime = "2019-01-31";
    private Random random = new Random();

    private List<String> phoneList = new ArrayList<>();
    private Map<String, String> phoneNameMap = new HashMap<>();

    private void initPhone(){
        phoneList.add("17078388295");
        phoneList.add("13980337439");
        phoneList.add("14575535933");
        phoneList.add("19902496992");
        phoneList.add("18549641558");
        phoneList.add("17005930322");
        phoneList.add("18468618874");
        phoneList.add("18576581848");
        phoneList.add("15978226424");
        phoneList.add("15542823911");
        phoneList.add("17526304161");
        phoneList.add("15422018558");
        phoneList.add("17269452013");
        phoneList.add("17764278604");
        phoneList.add("15711910344");
        phoneList.add("15714728273");
        phoneList.add("16061028454");
        phoneList.add("16264433631");
        phoneList.add("17601615878");
        phoneList.add("15897468949");

        phoneNameMap.put("17078388295", "李雁");
        phoneNameMap.put("13980337439", "卫艺");
        phoneNameMap.put("14575535933", "仰莉");
        phoneNameMap.put("19902496992", "陶欣悦");
        phoneNameMap.put("18549641558", "施梅梅");
        phoneNameMap.put("17005930322", "金虹霖");
        phoneNameMap.put("18468618874", "魏明艳");
        phoneNameMap.put("18576581848", "华贞");
        phoneNameMap.put("15978226424", "华啟倩");
        phoneNameMap.put("15542823911", "仲采绿");
        phoneNameMap.put("17526304161", "卫丹");
        phoneNameMap.put("15422018558", "戚丽红");
        phoneNameMap.put("17269452013", "何翠柔");
        phoneNameMap.put("17764278604", "钱溶艳");
        phoneNameMap.put("15711910344", "钱琳");
        phoneNameMap.put("15714728273", "缪静欣");
        phoneNameMap.put("16061028454", "焦秋菊");
        phoneNameMap.put("16264433631", "吕访琴");
        phoneNameMap.put("17601615878", "沈丹");
        phoneNameMap.put("15897468949", "褚美丽");
    }

    /**
     * 生产数据：17526304161,17764278604,2018-09-09 04:51:15,0043
     * 对应字段：caller,callee,buildTime,duration
     * */
    private String product(){
        String caller = null;
        String callerName = null;
        String callee = null;
        String calleeName = null;

        int callerIndex = (int)(Math.random()*phoneList.size());
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);

        while(true){
            int calleeIndex = (int)(Math.random()*phoneList.size());
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);
            if(!callee.equals(caller)){
                break;
            }
        }

        String buildTime = randomBuildTime(startTime,endTime);

        DecimalFormat df = new DecimalFormat("0000");
//        String duration = df.format((int)(30*60*Math.random()));
        String duration = df.format(random.nextInt(30*60));

        StringBuilder sb = new StringBuilder();
        sb.append(caller).append(",").append(callee).append(",")
            .append(buildTime).append(",").append(duration);
        return sb.toString();
    }

    private String randomBuildTime(String startTime, String endTime) {
        String buildTime = null;
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if(endDate.getTime()<=startDate.getTime()){
                return null;
            }
            long build = (long)((endDate.getTime()-startDate.getTime())*Math.random()+startDate.getTime());

            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            buildTime = sdf2.format(new Date(build));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return buildTime;
    }

    private void writeLog(String filePath){
        initPhone();
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath, true));
            while (true) {
                Thread.sleep(300);
                String log = product();
                System.out.println(log);
                osw.write(log+"\n");
                osw.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        if(args == null || args.length<=0){
            System.out.println("请输入日志文件全路径！");
            return;
        }
        ProductLog productLog = new ProductLog();
        productLog.writeLog(args[0]);
    }
}
