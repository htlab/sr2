package soxrecorderv2.common.model;

import java.util.ArrayList;
import java.util.List;

import soxrecorderv2.common.model.db.Record;
import soxrecorderv2.common.model.db.TValue;

public class RecordWithValues {
	
	private Record record;
	private List<TValue> values;
	
	public RecordWithValues(Record record, List<TValue> values) {
		this.record = record;
		this.values = values;
	}
	
	public RecordWithValues(Record record) {
		this.record = record;
		this.values = new ArrayList<>();
	}
	
	public Record getRecord() {
		return record;
	}
	
	public List<TValue> getValues() {
		return values;
	}
	
	public void addValue(TValue value) {
		values.add(value);
	}
	
	public void setValues(List<TValue> values) {
		this.values = values;
	}

}
