package template.aikafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerSingleton {
	private static final Logger log = LoggerFactory.getLogger(ProducerSingleton.class);

	public static final AiProducer instance = new AiProducer();

	public static AiProducer getInstance() {
		return instance;
	}
}
