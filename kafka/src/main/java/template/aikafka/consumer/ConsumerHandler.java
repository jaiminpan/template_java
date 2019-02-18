package template.aikafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerHandler {
	void handle_event(ConsumerRecord<String,Object>  it);
}
