package soxrecorderv2.common.model.db;

import java.util.Calendar;

import soxrecorderv2.common.model.ExportFormat;

/**
 * mapped with "export" table of PostgreSQL db schema.
 * (some fields are omitted because WebAPI does not return them)
 * 
 * @author tomotaka
 *
 */
public class Export {
	
	private final long id;
	private final Calendar timeStart;
	private final Calendar timeEnd;
	private final ExportFormat format;
	private final boolean isGzipped;
	private final boolean isIncludeXml;
	private final boolean isUsingRawValue;
	private final String fileName;
	private final Calendar saveUntil;
	private final Calendar created;
	private final int state;
	
	public Export(
				long id,
				Calendar timeStart,
				Calendar timeEnd,
				ExportFormat format,
				boolean isGzipped,
				boolean isIncludeXml,
				boolean isUsingRawValue,
				String fileName,
				Calendar saveUntil,
				Calendar created,
				int state
			) {
		this.id = id;
		this.timeStart = timeStart;
		this.timeEnd = timeEnd;
		this.format = format;
		this.isGzipped = isGzipped;
		this.isIncludeXml = isIncludeXml;
		this.isUsingRawValue = isUsingRawValue;
		this.fileName = fileName;
		this.saveUntil = saveUntil;
		this.created = created;
		this.state = state;
	}

	public long getId() {
		return id;
	}

	public Calendar getTimeStart() {
		return timeStart;
	}
	
	public boolean isFromBeginning() {
		return (timeStart == null);
	}

	public Calendar getTimeEnd() {
		return timeEnd;
	}
	
	public boolean isUntilLatest() {
		return (timeEnd == null);
	}

	public ExportFormat getFormat() {
		return format;
	}

	public boolean isGzipped() {
		return isGzipped;
	}

	public boolean isIncludeXml() {
		return isIncludeXml;
	}
	
	public boolean isUsingRawValue() {
		return isUsingRawValue;
	}
	
	public String getFileName() {
		return fileName;
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
