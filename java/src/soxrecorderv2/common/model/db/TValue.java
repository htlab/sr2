package soxrecorderv2.common.model.db;

import java.math.BigDecimal;

/**
 * representing "transducer_raw_value" table row and "transducer_typed_value" table row
 * @author tomotaka
 *
 */
public class TValue {
	
	private long databaseId;
	private long transducerId;
	
	private TValType type;

	private String stringValue;
	private long intValue;
	private double floatValue;
	private BigDecimal decimalValue;
	private long largeObjectId;
	
	private byte[] largeObject;
	
	private boolean hasSameTypedValue;
	
	public TValue(long databaseId, long transducerId, TValType type, String stringValue, long intValue, double floatValue, BigDecimal decimalValue, long largeObjectId, boolean hasSameTypedValue) {
		this.databaseId = databaseId;
		this.transducerId = transducerId;
		this.type = type;
		this.stringValue = stringValue;
		this.floatValue = floatValue;
		this.decimalValue = decimalValue;
		this.largeObjectId = largeObjectId;
		this.hasSameTypedValue = hasSameTypedValue;
	}

	public TValue(long transducerId, TValType type, String stringValue, long intValue, double floatValue, BigDecimal decimalValue, long largeObjectId, boolean hasSameTypedValue) {
		this(0, transducerId, type, stringValue, intValue, floatValue, decimalValue, largeObjectId, hasSameTypedValue);
	}
	
	public void setDatabaseId(long databaseId) {
		this.databaseId = databaseId;
	}
	
	public long getDatabaseId() {
		return databaseId;
	}

	public long getTransducerId() {
		return transducerId;
	}
	
	public TValType getType() {
		return type;
	}
	
	public String getStringValue() {
		return stringValue;
	}
	
	public long getIntValue() {
		return intValue;
	}
	
	public double getFloatValue() {
		return floatValue;
	}
	
	public BigDecimal getDecimalValue() {
		return decimalValue;
	}
	
	public long getLargeObjectId() {
		return largeObjectId;
	}
	
	public byte[] getLargeObject() {
		return largeObject;
	}
	
	public void setLargeObject(byte[] largeObject) {
		this.largeObject = largeObject;
	}
	
	public boolean hasSameTypedValue() {
		return hasSameTypedValue;
	}
	
	public void setHasSameTypedValue(boolean hasSameTypedValue) {
		this.hasSameTypedValue = hasSameTypedValue;
	}
	
}