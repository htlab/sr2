package soxrecorderv2.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import jp.ac.keio.sfc.ht.sox.protocol.TransducerValue;
import soxrecorderv2.common.model.NodeIdentifier;

public class NodeInfo {
	
	private final NodeIdentifier nodeId;
	
	private final long observationId;
	
	private final Map<String, Long> transducerIdMap;
	
	public NodeInfo(final NodeIdentifier nodeId, final long observationId, final Map<String, Long> transducerIdMap) {
		this.nodeId = nodeId;
		this.observationId = observationId;
		this.transducerIdMap = Collections.unmodifiableMap(transducerIdMap);
	}
	
	public NodeIdentifier getNodeId() {
		return nodeId;
	}
	
	public long getObservationId() {
		return observationId;
	}
	
	public Map<String, Long> getTransducerIdMap() {
		return transducerIdMap;
	}
	
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof NodeInfo)) {
			return false;
		}
		
		NodeInfo otherInfo = (NodeInfo)other;
		return equals(otherInfo.getNodeId(), otherInfo.getObservationId(), otherInfo.getTransducerIdMap());
	}
	
	public boolean isCovering(Collection<TransducerValue> tValues) {
		Set<String> tIdentifiers = new HashSet<>();
		for (TransducerValue tValue : tValues) {
			tIdentifiers.add(tValue.getId());
		}
		
		return tIdentifiers.equals(transducerIdMap.keySet());
	}
	
		
	public boolean equals(NodeIdentifier nodeId, long obid, Map<String, Long> tdrIdMap) {
		if (!nodeId.equals(this.nodeId)) {
			return false;
		}
		
		if (obid != observationId) {
			return false;
		}
		
		final Set<String> givenKeys = tdrIdMap.keySet();
		if (!givenKeys.equals(transducerIdMap.keySet())) {
			return false;
		}
		
		for (String k : givenKeys) {
			if (tdrIdMap.get(k) != transducerIdMap.get(k)) {
				return false;
			}
		}
		
		return true;
	}

}
