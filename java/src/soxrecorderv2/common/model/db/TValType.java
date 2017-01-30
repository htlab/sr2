package soxrecorderv2.common.model.db;

public enum TValType {
	String(1),
	Int(2),
	Float(3),
	Deciaml(4),
	LargeObject(5)
	;
	
	public static TValType typeOf(int type) {
		switch (type) {
		case 1:
			return TValType.String;
		case 2:
			return TValType.Int;
		case 3:
			return TValType.Float;
		case 4:
			return TValType.Deciaml;
		case 5:
			return TValType.LargeObject;
		default:
			throw new IllegalArgumentException();
		}
	}
	
	private int type;
	
	TValType(final int type) {
		this.type = type;
	}
	
	public int getType() {
		return type;
	}
}