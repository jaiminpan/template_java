package template.aikafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerSingleton {
	private static final Logger log = LoggerFactory.getLogger(ConsumerSingleton.class);

	public static final AiConsumer instance = new AiConsumer();

	public static AiConsumer getInstance() {
		return instance;
	}
}
