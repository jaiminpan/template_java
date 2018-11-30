package template.EventQueue;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventQueue {
	private static final Logger log = LoggerFactory.getLogger(EventQueue.class);

	Disruptor disruptor;

	int m_maxQueueSz = 10;

	public EventQueue() {
		this(100, 1);
	}

	public EventQueue(int maxQueueSz, int numThreads) {

		m_maxQueueSz = maxQueueSz;
		m_numThreads = numThreads;

		ThreadFactory threadFactory = Executors.defaultThreadFactory();

		disruptor = new Disruptor(m_eventFactory, normalizeBufferSize(m_maxQueueSz), threadFactory,
											ProducerType.MULTI, new BlockingWaitStrategy());

		QueueProcessorWorkHandler[] handlers = new QueueProcessorWorkHandler[m_numThreads];

		for (int i = 0; i < m_numThreads; i++) {
			handlers[i] = new QueueProcessorWorkHandler();
		}

		disruptor.handleEventsWithWorkerPool(handlers);

		// Start the Disruptor, starts all threads running
		m_ringBuffer = disruptor.start();
	}
	
	private static class RequestHolder<T> {
		private T item;

		public T remove() {
			T t = item;
			item = null;
			return t;
		}

		public void put(T event) {
			this.item = event;
		}
	}

	private static class RequestHolderFactory<T> implements EventFactory<RequestHolder<T>> {
	
		@Override
		public RequestHolder<T> newInstance() {
			return new RequestHolder<T>();
		}

	}

	private static class QueueProcessorWorkHandler implements
			WorkHandler<RequestHolder<Runnable>> {

		@Override
		public void onEvent(RequestHolder<Runnable> event) throws Exception {
		
			Runnable req = event.remove();
		
			req.run();

		}
	}

	// worker pool related items
	private RequestHolderFactory<Runnable> m_eventFactory = new RequestHolderFactory<Runnable>();
	private RingBuffer m_ringBuffer;
	private int m_numThreads = 1;

	/**
	 * @param bufferSize
	 * @return
	 */
	private int normalizeBufferSize(int bufferSize) {
		if (bufferSize <= 0) {
			return 8192;
		}
		int ringBufferSize = 2;
		while (ringBufferSize < bufferSize) {
			ringBufferSize *= 2;
		}
		return ringBufferSize;
	}

	public boolean processRequest(Runnable req) {
		long seq;

		seq = m_ringBuffer.next();

		RequestHolder<Runnable> item = (RequestHolder<Runnable>) m_ringBuffer.get(seq);

		item.put(req);

		m_ringBuffer.publish(seq);

		return true;

	}

	public long getAvailableCapacity() {
		return m_ringBuffer.remainingCapacity();
	}
	
	public long getBufferSize() {
		return disruptor.getBufferSize();
	}
}
