package template.aikafka.consumer;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import static java.time.temporal.ChronoUnit.MINUTES;

public class AiConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AiConsumer.class);

	private String consumerName = "Default Consumer Name";

	private long timeout = 1000;

	private ConcurrentHashMap<String, KafkaConsumer<String, Object>> topicMap = new ConcurrentHashMap<>();

	public static ExecutorService executors = Executors.newCachedThreadPool((r) -> {
		Thread thread = Executors.defaultThreadFactory().newThread(r);
		thread.setDaemon(true);
		return thread;
	});

	public void init(String kafka, String topic, String clientId, String group) {
		if (Strings.isNullOrEmpty(topic)) {
			LOGGER.error("Wrong topic name");
			return;
		}

		if (topicMap.containsKey(topic)) {
			LOGGER.warn("Consumer exists, don't init");
			return;
		}

		try {
			Properties props = new Properties();
			props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
			props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			//1.0.0
			//props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
			//props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
			props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

			KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
			topicMap.put(topic, consumer);
			LOGGER.info("Consumer init " + topic);
		} catch (Exception e) {
			LOGGER.error("Consumer init exception " + topic, e);
		}
	}

	/**
	 * Consume from last offset
	 */
	public void consume(String topic, ConsumerHandler handler) {
		KafkaConsumer<String, Object> consumer = topicMap.get(topic);
		if (consumer == null) {
			LOGGER.error("Consumer not init");
			return;
		}

		consumer.subscribe(Collections.singletonList(topic));
		LOGGER.info("Consumer Start " + topic);

		Thread thread = new Thread(() -> {

			boolean running = true;
			while (running) {
				try {
					ConsumerRecords<String, Object> records = consumer.poll(timeout);

					for (ConsumerRecord<String, Object> record : records) {
						try {
							handler.handle_event(record);
						} catch (Exception e) {
							LOGGER.error(e.getMessage(), e);
						}
					}

					try {
						if (!records.isEmpty()) {
							consumer.commitSync();
						}
					} catch (CommitFailedException e) {
						// specific failure handling
						LOGGER.error(e.getMessage(), e);
					}

				} catch (WakeupException e) {
					// shutdown
					running = false;
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);

					try {
						Thread.sleep(1000 * 60);
					} catch(InterruptedException ex) {
					}
				}
			}

			consumer.commitSync();
			consumer.close();
			LOGGER.info("Done shutdown consumer");
		});

//		thread.setDaemon(true);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				LOGGER.info("Shutdowning consumer...");
				consumer.wakeup();
				try {
					thread.join();
				} catch (InterruptedException e) {
				}
			}
		});
	}

	/**
	 * Consume from last offset
	 */
	public void consume(String topic, Class<? extends ConsumerHandler> clazz) {
		KafkaConsumer<String, Object> consumer = topicMap.get(topic);
		if (consumer == null) {
			LOGGER.error("Consumer not init");
			return;
		}

		consumer.subscribe(Collections.singletonList(topic));
		LOGGER.info("Consumer Start " + topic);
		Thread thread = new Thread(() -> {
			ConsumerHandler handler = null;
			while (true) {
				ConsumerRecords<String, Object> records = consumer.poll(timeout);

				for (ConsumerRecord<String, Object> record : records) {
					try {
						handler = clazz.newInstance();
					} catch (InstantiationException | IllegalAccessException e) {
						LOGGER.error("#Create ConsumerHandler failed:", e);
					}
					handler.handle_event(record);
				}
			}
		});
		thread.setDaemon(true);
		thread.start();
	}

	/**
	 * Consume from beginning offset
	 */
	public void consumeFromBegining(String topic, Class<? extends ConsumerHandler> clazz) {
		try{
			KafkaConsumer<String, Object> consumer = topicMap.get(topic);
			if (consumer == null) {
				LOGGER.error("Consumer not init");
				return;
			}

			consumer.subscribe(Collections.singletonList(topic));
			Thread thread = new Thread(() -> {
				ConsumerHandler handler = null;
				while (true) {
					ConsumerRecords<String, Object> records = consumer.poll(timeout);
					//reset offset
					Set<TopicPartition> assignments = consumer.assignment();
					assignments.forEach(topicPartition ->consumer.seekToBeginning(
							Collections.singletonList(topicPartition)));

					for (ConsumerRecord<String, Object> record : records) {
						try {
							handler = clazz.newInstance();
						} catch (InstantiationException | IllegalAccessException e) {
							LOGGER.error("#Create ConsumerHandler failed:", e);
						}
						handler.handle_event(record);
					}
				}
			});
			thread.setDaemon(true);
			thread.start();

		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}


	/**
	 * Consume from latest offset
	 */
	public void consumeFromLastest(String topic, Class<? extends ConsumerHandler> clazz) {
		try{
			KafkaConsumer<String, Object> consumer = topicMap.get(topic);
			if (consumer == null) {
				LOGGER.error("Consumer not init");
				return;
			}

			consumer.subscribe(Collections.singletonList(topic));

			Thread thread = new Thread(() -> {
				ConsumerHandler handler = null;
				while (true) {
					ConsumerRecords<String, Object> records = consumer.poll(timeout);
					//seek to end offset
					Set<TopicPartition> assignments = consumer.assignment();
					assignments.forEach(topicPartition ->consumer.seekToEnd(Collections.singletonList(topicPartition)));

					for (ConsumerRecord<String, Object> record : records) {
						try {
							handler = clazz.newInstance();
						} catch (InstantiationException | IllegalAccessException e) {
							LOGGER.error("#Create ConsumerHandler failed:", e);
						}
						handler.handle_event(record);
					}
				}
			});
			thread.setDaemon(true);
			thread.start();

		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	public void consumeFromTimes(String topic, int minute, ConsumerHandler handler) {
		try{
			KafkaConsumer<String, Object> consumer = topicMap.get(topic);
			if (consumer == null) {
				LOGGER.error("Consumer not init");
				return;
			}

			consumer.subscribe(Collections.singletonList(topic));

			Set<TopicPartition> assignments = consumer.assignment();
			Map<TopicPartition, Long> query = new HashMap<>();
			for (TopicPartition topicPartition : assignments) {
				//在每一分区上寻找对应的offset
				query.put(topicPartition, Instant.now().minus(minute, MINUTES).toEpochMilli());
			}
			Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);
			//根据找到的offset修改,没有则从最新的offset开始
			result.entrySet().stream().forEach(entry ->
					consumer.seek(entry.getKey(), Optional.ofNullable(entry.getValue())
							.map(OffsetAndTimestamp::offset)
							.orElse(new Long(Long.MAX_VALUE)))
			);

			Thread thread = new Thread(() -> {
				while (true) {
					ConsumerRecords<String, Object> records = consumer.poll(1000);

					for (ConsumerRecord<String, Object> record : records) {
						try {
							handler.handle_event(record);
						} catch (Exception e) {
							LOGGER.error("", e);
						}
					}
				}

			});
			thread.setDaemon(true);
			thread.start();

		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	public static void main(String[] args) {

		try{
			Properties props = new Properties();
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "testclient2");
			props.put(ConsumerConfig.CLIENT_ID_CONFIG, "testclient2");
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.97.7.138:9092,47.97.5.96:9092,47.97.9.120:9092");
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

			//1.0.0
			//props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
			props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");

			KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
			LOGGER.info("消费端初始化");
			consumer.subscribe(Collections.singletonList("topic_metric_tracking"));
			LOGGER.info("消费端开始消费" + "topic_metric_tracking");
			Thread thread = new Thread(() -> {
				while (true) {
					ConsumerRecords<String, Object> records = consumer.poll(1000);

					for (ConsumerRecord<String, Object> record : records) {
						System.out.println("time:"+System.currentTimeMillis()+",partition:"+record.partition()+",record:" + record.value());
					}
				}
			});
			thread.setDaemon(true);
			thread.start();
			Thread.currentThread().join();
		}catch(Exception e){
			e.printStackTrace();
			LOGGER.error("消费端初始化异常");
		}
	}
}
