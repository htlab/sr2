package soxrecorderv2.common.model;

import jp.ac.keio.sfc.ht.sox.protocol.Data;

public class RecordTask {
	
	private final String observationId;
	private final NodeIdentifier nodeId;
	private final Data data;
	
	public RecordTask(String observationId, NodeIdentifier nodeId, Data data) {
		this.observationId = observationId;
		this.nodeId = nodeId;
		this.data = data;
	}
	
	public String getObservationId() {
		return observationId;
	}
	
	public NodeIdentifier getNodeId() {
		return nodeId;
	}
	
	public Data getData() {
		return data;
	}

}
