package soxrecorderv2.common.model.db;

public class Transducer {
	
	public final long databaseId;
	public final long observationId;
	public final String name;
	public final String transducerId;
	
	public Transducer(long databaseId, long observationId, String name, String transducerId) {
		this.databaseId = databaseId;
		this.observationId = observationId;
		this.name = name;
		this.transducerId = transducerId;
	}
	
	public long getDatabaseId() {
		return databaseId;
	}
	
	public long getObservationId() {
		return observationId;
	}
	
	public String getName() {
		return name;
	}
	
	public String getTransducerId() {
		return transducerId;
	}

}
