package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties properties = null;
    static{
        // 读取配置文件、方便维护
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("");

        properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**回车
     * 获取参数值
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return properties.getProperty(key);
    }

}
