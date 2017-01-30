package soxrecorderv2.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {

	public static byte[] compress(byte[] original) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		GZIPOutputStream gzout = new GZIPOutputStream(bout);
		gzout.write(original);
		gzout.close();
		return bout.toByteArray();
	}
	
	public static byte[] uncompress(byte[] compressed, long contentLength) throws IOException {
		ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
		GZIPInputStream gzin = new GZIPInputStream(bin);
		byte[] buffer = new byte[(int)contentLength];
		gzin.read(buffer);
		gzin.close();
		return buffer;
	}

}
