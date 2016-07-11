package soxrecorderv2.junk;

import java.io.IOException;
import java.util.List;

import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smackx.pubsub.LeafNode;
import org.jivesoftware.smackx.pubsub.Subscription;

import jp.ac.keio.sfc.ht.sox.soxlib.SoxConnection;

public class UnsubscribeAllDevice {

	public static void main(String[] args) throws SmackException, IOException, XMPPException {
		new UnsubscribeAllDevice(args[0],args[1]);
	}

	public UnsubscribeAllDevice(String jid,String password) throws SmackException, IOException, XMPPException {
		
		String[] servers = {"sox.ht.sfc.keio.ac.jp", "soxfujisawa.ht.sfc.keio.ac.jp"};
		for (String server : servers) {
			System.out.println("connecting to " + server);
			SoxConnection con = new SoxConnection(server, jid, password, true);
			System.out.println("connected: " + server);

			// test
			try {
				System.out.println("fetching subscriptions...");
				List<Subscription> subs = con.getPubSubManager().getSubscriptions();
				System.out.println("fetching subscriptioons finished");
			
				System.out.println(subs.size());
				int i = 0;
				for (Subscription sub : subs) {
					System.out.println("unsubscribing: " +sub.getNode());
					LeafNode test = con.getPubSubManager().getNode(sub.getNode());
				
					test.unsubscribe(sub.getJid(), sub.getId());
					i++;
				}
				System.out.println("!!!! all finished !!!! server=" + server + ", N=" + i);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				con.disconnect();
			}
		}
		
		System.out.println("!!!!!!!! everything was finished");
	}

}