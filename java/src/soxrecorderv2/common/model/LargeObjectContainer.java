package soxrecorderv2.common.model;

import jp.ac.keio.sfc.ht.sox.protocol.TransducerValue;
import soxrecorderv2.util.HashUtil;
import soxrecorderv2.util.MyCharSet;

public class LargeObjectContainer {
	
	private final TransducerValue transducerValue;
	private final byte[] data;
	private final String hash;
	private boolean isRaw;

	public LargeObjectContainer(final TransducerValue transducerValue, boolean useRaw) {
		this.transducerValue = transducerValue;
		this.isRaw = useRaw;
		if (useRaw) {
			this.data = transducerValue.getRawValue().getBytes(MyCharSet.UTF8);
		} else {
			this.data = transducerValue.getTypedValue().getBytes(MyCharSet.UTF8);
		}
		this.hash = HashUtil.sha256(data);
	}
	
	public TransducerValue getTransducerValue() {
		return transducerValue;
	}
	
	public byte[] getData() {
		return this.data;
	}
	
	public String getHash() {
		return hash;
	}
	
	public boolean isRaw() {
		return isRaw;
	}

}
