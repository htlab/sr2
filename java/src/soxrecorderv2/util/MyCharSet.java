package soxrecorderv2.util;

import java.nio.charset.Charset;

public class MyCharSet {
	
	public static Charset UTF8;
	
	static {
		Charset utf8 = null;
		
		utf8 = Charset.forName("UTF-8");
		
		UTF8 = utf8;
	}

}
