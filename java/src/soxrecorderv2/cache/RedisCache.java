package soxrecorderv2.cache;

import java.io.Closeable;
import java.io.IOException;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.util.BinaryUtil;
import soxrecorderv2.util.MyCharSet;

public class RedisCache implements SoxRecorderCache, Closeable {
	
	private static final byte[] NIL = "nil".getBytes();
	
	private String host;
	private BinaryJedis conn;
	private int retryCount = 5;
	
	public RedisCache(String host) {
		this.host = host;
	}
	
	public void open() {
		conn = new BinaryJedis(host);
	}
	
	@Override
	public void close() {
		if (conn == null) {
			return;
		}
		
		conn.close();
		conn = null;
	}

	@Override
	public long getObservationId(NodeIdentifier nodeId) throws CacheMissException {
		byte[] key = buildKeyForObservationId(nodeId);
		byte[] binObservationId = get(key);
		try {
			return BinaryUtil.bin2long(binObservationId);
		} catch (IOException e) {
			e.printStackTrace();
			return 0; // FIXME
		}
	}

	@Override
	public long getTransducerId(NodeIdentifier nodeId, String transducerId) throws CacheMissException {
		byte[] key = buildKeyForTransducerId(nodeId, transducerId);
		byte[] binTransducerId = get(key);
		try {
			return BinaryUtil.bin2long(binTransducerId);
		} catch (IOException e) {
			e.printStackTrace();
			return 0;  // FIXME
		}
	}

	@Override
	public void setObservationId(NodeIdentifier nodeId, long observationId) {
		byte[] key = buildKeyForObservationId(nodeId);
		byte[] value = BinaryUtil.long2bin(observationId);
		set(key, value);
	}

	@Override
	public void setTransducerId(NodeIdentifier nodeId, String transducerId, long transducerDatabsaeId) {
		byte[] key = buildKeyForTransducerId(nodeId, transducerId);
		byte[] value = BinaryUtil.long2bin(transducerDatabsaeId);
		set(key, value);
	}

	@Override
	public void removeObservationId(NodeIdentifier nodeId) {
		byte[] key = buildKeyForObservationId(nodeId);
		remove(key);
	}

	@Override
	public void removeTransducerId(NodeIdentifier nodeId, String transducerId) {
		byte[] key = buildKeyForTransducerId(nodeId, transducerId);
		remove(key);
	}
	
	private byte[] buildKeyForObservationId(NodeIdentifier nodeId) {
		StringBuilder sb = new StringBuilder();
		sb.append("sr2:obid|||");
		sb.append(nodeId.getServer());
		sb.append("|||");
		sb.append(nodeId.getNode());
		return sb.toString().getBytes(MyCharSet.UTF8);
	}
	
	private byte[] buildKeyForTransducerId(NodeIdentifier nodeId, String transducerId) {
		StringBuilder sb = new StringBuilder();
		sb.append("sr2:tdrid|||");
		sb.append(nodeId.getServer());
		sb.append("|||");
		sb.append(nodeId.getNode());
		sb.append("|||");
		sb.append(transducerId);
		return sb.toString().getBytes(MyCharSet.UTF8);
	}
	
	private byte[] get(byte[] key) throws CacheMissException {
		try {
			byte[] bytes = conn.get(key);
			if (isNil(bytes)) {
				throw new CacheMissException();
			}
			return bytes;
		} catch (JedisConnectionException e) {
			int failed = 1;
			while (failed < retryCount) {
				try {
					return conn.get(key);
				} catch (JedisConnectionException e2) {
					failed++;
				}
			}
			// FIXME: logging
			return null;
		}
	}
	
	private void set(byte[] key, byte[] value) {
		try {
			conn.set(key, value);
		} catch (JedisConnectionException e) {
			int failed = 1;
			while (failed < retryCount) {
				try {
					conn.set(key, value);
					return;
				} catch (JedisConnectionException e2) {
					failed++;
				}
			}
			// FIXME: logging
		}
	}
	
	private void remove(byte[] key) {
		try {
			conn.del(key);
		} catch (JedisConnectionException e) {
			int failed = 1;
			while (failed < retryCount) {
				try {
					conn.del(key);
					return;
				} catch (JedisConnectionException e2) {
					failed++;
				}
			}
			// FIXME: logging
		}
	}
	
	private boolean isNil(byte[] test) {
		if (test.length != NIL.length) {
			return false;
		}
		for (int i = 0; i < NIL.length; i++) {
			if (test[i] != NIL[i]) {
				return false;
			}
		}
		return true;
	}

}
