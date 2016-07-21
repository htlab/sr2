package soxrecorderv2.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class BinaryUtil {
	
	public static long bin2long(byte[] longData) throws IOException {
		ByteArrayInputStream bIn = new ByteArrayInputStream(longData);
		DataInputStream dataIn = new DataInputStream(bIn);
		long ret = 0;
		ret = dataIn.readLong();
		return ret;
	}
	
	public static byte[] long2bin(long longValue) {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bOut);
		try {
			dataOut.writeLong(longValue);
			dataOut.close();
		} catch (IOException e) {
			
		}
		byte[] ret = bOut.toByteArray();
		return ret;
	}

}
