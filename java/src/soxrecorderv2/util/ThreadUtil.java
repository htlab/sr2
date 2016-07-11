package soxrecorderv2.util;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ThreadUtil {
	
	public static void countDownLatchAwait(CountDownLatch countDownLatch) {
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void countDownLatchAwait(CountDownLatch countDownLatch, long timeoutMsec) {
		try {
			countDownLatch.await(timeoutMsec, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void joinAll(Collection<Thread> threads) {
		for (Thread t : threads) {
			join(t);
		}
	}
	
	public static void join(Thread t) {
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void sleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
