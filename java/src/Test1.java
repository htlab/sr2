
public class Test1 {
	
	public static class ThreadA extends Thread {
		public void run() {
			System.out.println("a start");
			
			ThreadB b = new ThreadB();
			b.start();
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("a finish?");
		}
	}
	
	public static class ThreadB extends Thread {
		public void run() {
			System.out.println("b start");
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("b finish");
		}
	}

	public static void main(String[] args) {
		ThreadA a = new ThreadA();
		a.start();
		try {
			a.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
