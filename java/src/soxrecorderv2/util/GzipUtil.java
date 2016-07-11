package soxrecorderv2.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {

	public static byte[] compress(byte[] original) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		GZIPOutputStream gzout = new GZIPOutputStream(bout);
		gzout.write(original);
		gzout.close();
		return bout.toByteArray();
	}

}
