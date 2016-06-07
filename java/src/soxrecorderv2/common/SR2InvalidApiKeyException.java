package soxrecorderv2.common;

public class SR2InvalidApiKeyException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public SR2InvalidApiKeyException() {
		super();
	}
	
	public SR2InvalidApiKeyException(String s) {
		super(s);
	}
	
	public SR2InvalidApiKeyException(String s, Throwable t) {
		super(s, t);
	}

}
