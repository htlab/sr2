package soxrecorderv2.cache;

public class CacheMissException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public CacheMissException() {
		super();
	}
	
	public CacheMissException(String msg) {
		super(msg);
	}
	
	public CacheMissException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
