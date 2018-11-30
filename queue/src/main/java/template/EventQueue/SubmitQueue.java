package template.EventQueue;

public class SubmitQueue {

	private EventQueue queue = new EventQueue(100, 10);

	private static SubmitQueue self;

	public static SubmitQueue instance() {
		if (self == null)
			self = new SubmitQueue();
		return self;
	}

	public void put(Runnable req) {
		queue.processRequest(req);
	}

}
