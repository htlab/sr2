package soxrecorderv2.junk;

import java.io.IOException;
import java.util.List;

import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;

import jp.ac.keio.sfc.ht.sox.soxlib.SoxConnection;

public class ShowAllSensors {
	public static void main(String[] args) throws SmackException, IOException, XMPPException {
		SoxConnection conn = new SoxConnection("sox.ht.sfc.keio.ac.jp", false);
		
		int more255 = 0;
		List<String> sensors = conn.getAllSensorList();
		int all = sensors.size();
		for (String s : sensors) {
			if (s.length() > 255) {
				System.out.println("len=" + s.length() + ", node=" + s);
				more255++;
			}
		}
		
		conn.disconnect();
		System.out.println("255<: " + more255);
		System.out.println("all: " + all);
	}
}
