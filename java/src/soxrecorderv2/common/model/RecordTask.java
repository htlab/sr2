package soxrecorderv2.common.model;

import jp.ac.keio.sfc.ht.sox.protocol.Data;

public class RecordTask {
	
	private final NodeIdentifier nodeId;
	private final Data data;
	private String rawXml;
	
	public RecordTask(NodeIdentifier nodeId, Data data, String rawXml) {
		this.nodeId = nodeId;
		this.data = data;
		this.rawXml = rawXml;
	}
	
	public NodeIdentifier getNodeId() {
		return nodeId;
	}
	
	public Data getData() {
		return data;
	}
	
	public String getRawXml() {
		return rawXml;
	}

}
