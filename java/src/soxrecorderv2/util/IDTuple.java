package soxrecorderv2.util;

/**
 * 2つのlongをmapのキーにしたかった。
 * @author tomotaka
 *
 */
public class IDTuple {
	
	private long id1;
	private long id2;
	
	public IDTuple(final long id1, final long id2) {
		this.id1 = id1;
		this.id2 = id2;
	}
	
	public long getId1() {
		return id1;
	}
	
	public long getId2() {
		return id2;
	}
	
	@Override
	public boolean equals(Object other) {
		if (!(other instanceof IDTuple)) {
			return false;
		}
		
		IDTuple o = (IDTuple)other;
		return (o.id1 == id1 && o.id2 == id2);
	}
	
	@Override
	public int hashCode() {
		return 0x19851985 ^ Long.valueOf(id1).hashCode() ^ Long.valueOf(id2).hashCode();
	}

}
