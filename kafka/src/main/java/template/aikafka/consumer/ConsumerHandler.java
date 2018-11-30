package template.aikafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerHandler {
	public abstract void handle_event(ConsumerRecord<String,Object>  it);
}
