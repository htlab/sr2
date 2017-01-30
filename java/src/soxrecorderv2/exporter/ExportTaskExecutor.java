package soxrecorderv2.exporter;

import soxrecorderv2.common.model.ExportFormat;
import soxrecorderv2.common.model.ExportTask;
import soxrecorderv2.common.model.db.Export;
import soxrecorderv2.exporter.formatimpl.CSVExporter;
import soxrecorderv2.exporter.formatimpl.FormatExporter;
import soxrecorderv2.exporter.formatimpl.JSONExporter;

public class ExportTaskExecutor implements Runnable {
	
	private final Exporter parent;
	private final ExportTask task;
	private FormatExporter exporter;
	
	public ExportTaskExecutor(Exporter parent, ExportTask task) {
		this.parent = parent;
		this.task = task;
	}

	@Override
	public void run() {
		final Export exportProfile = task.getExportProfile();
		final ExportFormat format = exportProfile.getFormat();
		
		if (format == ExportFormat.CSV) {
			exporter = new CSVExporter(parent, task);
		} else if (format == ExportFormat.JSON) {
			exporter = new JSONExporter(parent, task);
		} else {
			throw new IllegalArgumentException();
		}
		exporter.run();
	}
	
	public void stopExporting() {
		if (exporter != null) {
			exporter.stopExporting();
		}
	}

}
