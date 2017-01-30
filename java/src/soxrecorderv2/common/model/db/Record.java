package soxrecorderv2.common.model.db;

import java.sql.Timestamp;

public class Record {
	
	private long databaseId;
	private long observationId;
	private boolean isParseError;
	private Timestamp created;
	
	public Record(long databaseId, long observationId, boolean isParseError, Timestamp created) {
		this.databaseId = databaseId;
		this.observationId = observationId;
		this.isParseError = isParseError;
		this.created = created;
	}
	
	public long getDatabaseId() {
		return databaseId;
	}
	
	public long getObservationId() {
		return observationId;
	}
	
	public boolean isParseError() {
		return isParseError;
	}
	
	public Timestamp getCreated() {
		return created;
	}
	
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof Record)) {
			return false;
		}
		
		Record o = (Record)other;
		
		if (databaseId != o.databaseId) {
			return false;
		}
		
		if (observationId != o.observationId) {
			return false;
		}
		
		if (isParseError != o.isParseError) {
			return false;
		}
		
		if (!created.equals(o.created)) {
			return false;
		}
		
		return true;
	}

}
