package soxrecorderv2.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

public class ThrowableUtil {
	
	public static String convertToString(Throwable throwable) {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		PrintWriter writer = new PrintWriter(bOut);
		throwable.printStackTrace(writer);
		writer.flush();
		String ret = new String(bOut.toByteArray(), MyCharSet.UTF8);
		return ret;
	}
	
	
	private ThrowableUtil() {}

}
