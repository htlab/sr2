package soxrecorderv2.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtil {
	/**
	 * byte列contentの内容をSHA-256でハッシュダイジェストを求めて16進数の文字列で返す
	 * @param content
	 * @return
	 */
	public static String sha256(byte[] content) {
		// memo: http://qiita.com/rsuzuki/items/7e3bd8248c55dab8341d
		String algo = "SHA-256";
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance(algo);
		} catch (NoSuchAlgorithmException e) {
	
		}
		
		digest.update(content);
		
		byte[] binDigest = digest.digest();
		StringBuilder sb = new StringBuilder();
		for (byte b : binDigest) {
			sb.append(String.format("%02x", b));
		}
		return sb.toString();
	}

}
