package soxrecorderv2.common.model;

import java.util.Calendar;

/**
 * mapped with "export" table of PostgreSQL db schema.
 * (some fields are omitted because WebAPI does not return them)
 * 
 * @author tomotaka
 *
 */
public class Export {
	
	private final long id;
	private final long observationId;
	private final Calendar timeStart;
	private final Calendar timeEnd;
	private final String format;
	private final boolean isGzipped;
	private final boolean isIncludeXml;
//	private final String fileName;
	private final Calendar saveUntil;
	private final Calendar created;
	private final int state;
	
	public Export(
				long id,
				long observationId,
				Calendar timeStart,
				Calendar timeEnd,
				String format,
				boolean isGzipped,
				boolean isIncludeXml,
				Calendar saveUntil,
				Calendar created,
				int state
			) {
		this.id = id;
		this.observationId = observationId;
		this.timeStart = timeStart;
		this.timeEnd = timeEnd;
		this.format = format;
		this.isGzipped = isGzipped;
		this.isIncludeXml = isIncludeXml;
		this.saveUntil = saveUntil;
		this.created = created;
		this.state = state;
	}

	public long getId() {
		return id;
	}

	public long getObservationId() {
		return observationId;
	}

	public Calendar getTimeStart() {
		return timeStart;
	}

	public Calendar getTimeEnd() {
		return timeEnd;
	}

	public String getFormat() {
		return format;
	}

	public boolean isGzipped() {
		return isGzipped;
	}

	public boolean isIncludeXml() {
		return isIncludeXml;
	}

	public Calendar getSaveUntil() {
		return saveUntil;
	}

	public Calendar getCreated() {
		return created;
	}

	public int getState() {
		return state;
	}

}
