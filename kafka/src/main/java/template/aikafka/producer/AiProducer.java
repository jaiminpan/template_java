package template.aikafka.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AiProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AiProducer.class.getName());

	private ConcurrentHashMap<String, Producer<String, String>> topicMap = new ConcurrentHashMap<>();

	public AiProducer init(String kafka, String topic, String clientId) {

		if (Strings.isNullOrEmpty(kafka)) {
			LOGGER.error("not config kafka");
			return this;
		}

		if (Strings.isNullOrEmpty(topic)) {
			LOGGER.error("Wrong topic name");
			return this;
		}

		if (topicMap.containsKey(topic)) {
			return this;
		}

		try {
			//http://kafka.apache.org/documentation.html#producerconfigs
			Properties props = new Properties();
			// 触发acknowledgement机制,数据完整性相关
			// 值为0,1,all,可以参考
			props.put("acks", "all");
			props.put("retries", 3);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("compression.type", "gzip");
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
			props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			Producer<String, String> producer = new KafkaProducer<>(props);
			topicMap.put(topic, producer);
			LOGGER.info("Producer init " + topic);
		} catch (Exception e) {
			LOGGER.error("Producer init exception " + topic, e);
		}

		return this;
	}

	public void close(String topic) {
		if (topicMap.containsKey(topic)) {
			Producer<String, String> producer = topicMap.get(topic);
			producer.close();
			topicMap.remove(topic);
		}
	}

	public void send(String topic, String key, String message) {
		Producer<String, String> producer = topicMap.get(topic);
		if (producer == null) {
			LOGGER.info("Producer not init");
			return;
		}

		try {
			RecordMetadata ret = producer.send(new ProducerRecord<String, String>(topic, key, message)).get();
		} catch (Exception e) {
			LOGGER.error("kafka write failed", e);
		}
	}

	public static void main(String[] args) {
		Properties properties = new Properties();
		String kafka = "47.97.7.138:9092,47.97.5.96:9092,47.97.9.120:9092";
		String clientId = "testclient";
		String group = "testclient";

		try{
			Properties props = new Properties();
			// 触发acknowledgement机制,数据完整性相关
			// 值为0,1,all,可以参考
			props.put("acks", "all");
			props.put("retries", 3);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("compression.type","gzip");
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
			props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
			//props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			Producer<String, String> producer = new KafkaProducer<>(props);
			Map d = new HashMap<>();
			d.put("code","");
			d.put("ip",null);
			try {
				int i=0;
				while(i<100){
					RecordMetadata ret = producer.send(
							new ProducerRecord<String, String>("topic_metric_tracking", "key", d.toString())).get();
					System.out.println("写入成功");
					TimeUnit.SECONDS.sleep(1);
					i++;
					//break;
				}
			} catch (Exception e) {
				LOGGER.info("kafka 写入失败", e);
			}
			LOGGER.info("生产端初始化成功");
		}catch(Exception e){
			LOGGER.error("初始化Config异常");
		}
	}
}
