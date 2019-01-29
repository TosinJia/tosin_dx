package kafka;

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

/**
 * @author TosinJia
 */
public class HBaseConsumer {
    public static void main(String[] args){
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        HBaseDAO hBaseDAO = new HBaseDAO();
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String,String> record: records) {
                String key = record.key();
                String value = record.value();
                System.out.println(key+"\t"+value);
                //17269452013,15542823911,2018-08-28 11:58:23,0800
                hBaseDAO.put(value);
            }
        }
    }

}
