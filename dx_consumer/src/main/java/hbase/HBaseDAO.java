package hbase;

import utils.PropertiesUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HBaseDAO {
    private String namespace;
    private String tableName;
    private int regions;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("");

    public HBaseDAO(){
        namespace = PropertiesUtil.getProperty("");
        tableName = PropertiesUtil.getProperty("");
        regions = Integer.valueOf(PropertiesUtil.getProperty(""));
    }

    public void put(String ori){
        try {
            String[] splitOri = ori.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTS = String.valueOf(sdf1.parse(buildTime).getTime());




        } catch (ParseException e) {
            e.printStackTrace();
        }



    }
}
