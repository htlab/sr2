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
		return fetchAllSensors(conn);
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
		return fetchAllSensors(anonymousConn);
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
		return fetchAllSensors(conn);
	}

	public static Timestamp parseTransducerTimeStamp(TransducerValue tValue) throws ParseException {
		java.util.Date parsed = XmppDateTime.parseXEP0082Date(tValue.getTimestamp());
		java.sql.Timestamp timestamp = new java.sql.Timestamp(parsed.getTime());
		return timestamp;
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
