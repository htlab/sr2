package soxrecorderv2.common.model.db;

import java.io.IOException;

import soxrecorderv2.util.GzipUtil;

public class LargeObject {
	
	private long databaseId;
	private boolean isGzipped;
	private String hashKey;
	private byte[] content;
	private long contentLength;
	
	public LargeObject(
			long databaseId, boolean isGzipped, String hashKey,
			byte[] content, long contentLength) {
		this.databaseId = databaseId;
		this.isGzipped = isGzipped;
		this.hashKey = hashKey;
		this.content = content;
		this.contentLength = contentLength;
	}
	
	public long getDatabaseId() {
		return databaseId;
	}
	
	public boolean isGzipped() {
		return isGzipped;
	}
	
	public String getHashKey() {
		return hashKey;
	}
	
	public byte[] getRawContent() {
		return content;
	}
	
	public long getContentLength() {
		return contentLength;
	}
	
	public byte[] getRealContent() {
		if (isGzipped) {
			try {
				return GzipUtil.uncompress(content, contentLength);
			} catch (IOException e) {
				e.printStackTrace();
				return null;  // FIXME
			}
		} else {
			return content;
		}
	}

}
