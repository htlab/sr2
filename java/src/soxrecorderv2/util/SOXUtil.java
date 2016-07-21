package soxrecorderv2.util;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;

import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.SmackException.NoResponseException;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.XMPPException.XMPPErrorException;
import org.jxmpp.util.XmppDateTime;

import jp.ac.keio.sfc.ht.sox.protocol.TransducerValue;
import jp.ac.keio.sfc.ht.sox.soxlib.SoxConnection;
import soxrecorderv2.common.model.SoxLoginInfo;

public class SOXUtil {
	
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
