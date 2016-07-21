package soxrecorderv2.cache;

import soxrecorderv2.common.model.NodeIdentifier;

public interface SoxRecorderCache {
	
	public long getObservationId(NodeIdentifier nodeId) throws CacheMissException;
	
	public long getTransducerId(NodeIdentifier nodeId, String transducerId) throws CacheMissException;
	
	public void setObservationId(NodeIdentifier nodeId, long observationId);
	
	public void setTransducerId(NodeIdentifier nodeId, String transducerId, long transducerDatabsaeId);
	
	public void removeObservationId(NodeIdentifier nodeId);
	
	public void removeTransducerId(NodeIdentifier nodeId, String transducerId);

}
