package soxrecorderv2.exporter.formatimpl;

import java.util.Collection;

import soxrecorderv2.common.model.ExportTask;
import soxrecorderv2.common.model.db.Observation;
import soxrecorderv2.common.model.db.Record;
import soxrecorderv2.common.model.db.TValue;
import soxrecorderv2.exporter.AbstractExporter;
import soxrecorderv2.exporter.Exporter;

public class JSONExporter extends AbstractExporter {
	
	public JSONExporter(final Exporter parent, final ExportTask task) {
		initialize(parent, task);
	}

	@Override
	public String buildItemContent(Observation observation, Record record, Collection<TValue> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String buildItemContent(Observation observation, Record record, Collection<TValue> values, String rawXml) {
		// TODO Auto-generated method stub
		return null;
	}

}
