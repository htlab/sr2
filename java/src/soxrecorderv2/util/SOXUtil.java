package soxrecorderv2.util;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.SmackException.NoResponseException;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.XMPPException.XMPPErrorException;
import org.jxmpp.util.XmppDateTime;

import jp.ac.keio.sfc.ht.sox.protocol.TransducerValue;
import jp.ac.keio.sfc.ht.sox.soxlib.SoxConnection;
import soxrecorderv2.common.model.LargeObjectContainer;
import soxrecorderv2.common.model.SoxLoginInfo;

public class SOXUtil {

	public static final int VALUE_TYPE_STRING       = 1;
	public static final int VALUE_TYPE_INT          = 2;
	public static final int VALUE_TYPE_FLOAT        = 3;
	public static final int VALUE_TYPE_DECIMAL      = 4;
	public static final int VALUE_TYPE_LARGE_OBJECT = 5;

	public static final Pattern PATTERN_INT;
	public static final Pattern PATTERN_FLOAT;

	public static final String[] DECIMAL_NAMES_ARRAY = {
		"lat",
		"lng",
		"latitude",
		"longitude"
	};
	public static final Set<String> DECIMAL_NAMES;
	
	static {
		Pattern patInt = null;
		Pattern patFloat = null;
		
		patInt = Pattern.compile("\\A-?[1-9][0-9]*\\z");
		patFloat = Pattern.compile("\\A-?([0-9]|[1-9][0-9]*)\\.[0-9]+\\z");

		Set<String> decimalNames = new HashSet<>();
		for (String dn : DECIMAL_NAMES_ARRAY) {
			decimalNames.add(dn);
		}
		
		PATTERN_INT = patInt;
		PATTERN_FLOAT = patFloat;

		DECIMAL_NAMES = Collections.unmodifiableSet(decimalNames);
	}
	
	public static Collection<String> extractLargeObjectHashes(Collection<LargeObjectContainer> loContainers) {
		List<String> ret = new ArrayList<>();
		for (LargeObjectContainer loContainer : loContainers) {
			ret.add(loContainer.getHash());
		}
		return Collections.unmodifiableList(ret);
	}
	
	public static Collection<LargeObjectContainer> extractLargeObjects(Collection<TransducerValue> tValues) {
		List<LargeObjectContainer> ret = new ArrayList<>();
		for (TransducerValue tv : tValues) {
			if (tv == null) {
				continue;
			}
			
			if (isLargeObject(tv, true)) {
				ret.add(new LargeObjectContainer(tv, true));
			}
			
			if (isLargeObject(tv, false)) {
				ret.add(new LargeObjectContainer(tv, false));
			}
		}
		return Collections.unmodifiableList(ret);
	}
	
	public static Collection<LargeObjectContainer> uniqueLargeObjects(Collection<LargeObjectContainer> largeObjects) {
		Set<String> hashes = new HashSet<>();
		List<LargeObjectContainer> ret = new ArrayList<>();
		for (LargeObjectContainer loContainer : largeObjects) {
			String hash = loContainer.getHash();
			if (hashes.contains(hash)) {
				continue;
			}
			ret.add(loContainer);
			hashes.add(hash);
		}
		return ret;
	}
	
	public static int guessValueType(final TransducerValue tValue, boolean isRaw) {
		final String val = (isRaw) ? tValue.getRawValue() : tValue.getTypedValue();
		return guessValueType(tValue.getId(), val);
	}

	/**
	 * valueの形式からtypeを推測する。
	 * 場合によってはtransducerIdも推測に必要なので値としてとる。
	 * (transducerIdがlatやlngだったら緯度経度なので精度誤差を出さないためにVALUE_TYPE_DECIMALにするなど
	 * 
	 * @param transducerId
	 * @param value
	 * @return
	 */
	public static int guessValueType(String transducerId, String value) {
		if (255 < value.length()) {
			return VALUE_TYPE_LARGE_OBJECT;
		} else if (PATTERN_INT.matcher(value).matches()) {
			return VALUE_TYPE_INT;
		} else if (PATTERN_FLOAT.matcher(value).matches()) {
			if (isDecimalName(transducerId)) {
				return VALUE_TYPE_DECIMAL;
			} else {
				return VALUE_TYPE_FLOAT;
			}
		} else {
			return VALUE_TYPE_STRING;
		}
	}
	
	public static boolean isLargeObject(TransducerValue tValue, boolean isRaw) {
		final String val = (isRaw) ? tValue.getRawValue() : tValue.getTypedValue();
		return isLargeObject(tValue.getId(), val);
	}
	
	public static boolean isLargeObject(String transducerId, String value) {
		if (value == null) {
			return false;
		}
		return (255 < value.length());
	}

	public static boolean isDecimalName(String transducerId) {
		String lowerId = transducerId.toLowerCase();
		return DECIMAL_NAMES.contains(lowerId);
	}

	public static boolean hasSameTypedValue(TransducerValue tValue) {
		String raw = tValue.getRawValue();
		String typed = tValue.getTypedValue();
		if (raw == null && typed == null) {
			return true;
		}
		return raw.equals(typed);
	}
	
	public static Collection<TransducerValue> extractTypedValueDifferentFromRaw(final Collection<TransducerValue> tValues) {
		final List<TransducerValue> ret = new ArrayList<>();
		for (TransducerValue tValue : tValues) {
			if (!hasSameTypedValue(tValue)) {
				ret.add(tValue);
			}
		}
		return ret;
	}
	
	public static Collection<String> extractTransducerIds(final Collection<TransducerValue> tValues) {
		final List<String> ret = new ArrayList<>();
		for (TransducerValue tv : tValues) {
			ret.add(tv.getId());
		}
		return Collections.unmodifiableList(ret);
	}
	
	/**
	 * SoxサーバへのXMPPコネクションを開く
	 * @param soxLoginInfo
	 * @return
	 * @throws SmackException
	 * @throws IOException
	 * @throws XMPPException
	 */
	public static SoxConnection open(SoxLoginInfo soxLoginInfo) throws SmackException, IOException, XMPPException {
		if (soxLoginInfo.isAnonymous()) {
			return new SoxConnection(soxLoginInfo.getServer(), false);
		} else {
			return new SoxConnection(soxLoginInfo.getServer(), soxLoginInfo.getJid(), soxLoginInfo.getPassword(), false);
		}
		
	}
	
	public static List<String> fetchAllSensors(SoxLoginInfo soxLoginInfo) throws SmackException, IOException, XMPPException {
		SoxConnection conn = open(soxLoginInfo);
		try {
			return fetchAllSensors(conn);
		} finally {
			conn.disconnect();
		}
	}
	
	/**
	 * anonymous
	 * @param server
	 * @return
	 * @throws XMPPException 
	 * @throws IOException 
	 * @throws SmackException 
	 */
	public static List<String> fetchAllSensors(String server) throws SmackException, IOException, XMPPException {
		SoxConnection anonymousConn = new SoxConnection(server, false);
		try {
			return fetchAllSensors(anonymousConn);
		} finally {
			anonymousConn.disconnect();
		}
	}
	
	/**
	 * not anonymous
	 * @param server
	 * @param jid
	 * @param password
	 * @return
	 * @throws SmackException
	 * @throws IOException
	 * @throws XMPPException
	 */
	public static List<String> fetchAllSensors(String server, String jid, String password) throws SmackException, IOException, XMPPException {
		SoxConnection conn = new SoxConnection(server, jid, password, false);
		try {
			return fetchAllSensors(conn);
		} finally {
			conn.disconnect();
		}
	}

	public static Timestamp parseTransducerTimeStamp(TransducerValue tValue) throws ParseException {
		java.util.Date parsed = XmppDateTime.parseXEP0082Date(tValue.getTimestamp());
		java.sql.Timestamp timestamp = new java.sql.Timestamp(parsed.getTime());
		return timestamp;
	}
	
	public static void closeQueitly(SoxConnection conn) {
		if (conn != null) {
			conn.disconnect();
		}
	}
	
	private static List<String> fetchAllSensors(SoxConnection conn) throws NoResponseException, XMPPErrorException, NotConnectedException {
		try {
			return conn.getAllSensorList();
		} finally {
			conn.disconnect();
		}
	}
	
	private SOXUtil() {}  // you cannot instantiate this class

}
