package template.EventQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubmitRequest implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(SubmitRequest.class);

	public SubmitRequest() {

	}

	@Override
	public void run() {
		try {
			log.info("doing");

		} catch (Exception e) {
			log.error("", e);
		}
	}
}
