package soxrecorderv2.common.model.db;

import java.sql.Timestamp;

import soxrecorderv2.common.model.NodeIdentifier;
import soxrecorderv2.common.model.SoxLoginInfo;

public class Observation {
	
	private final long databaseId;
	private final String soxServer;
	private final String soxNode;
	private final String soxJid;
	private final String soxPassword;
	private final boolean isUsingDefaultUser;
	private final boolean isExisting;
	private final boolean isRecordStopped;
	private final boolean isSubscribed;
	private final Timestamp created;
	private final boolean isSubscribeFailed;
	private final double recentMonthlyAverageDataArrival;
	private final long recentMonthlyTotalDataArrival;
	private final long recentMonthlyDataArrivalDays;
	

	public Observation(
				long databaseId,
				String soxServer,
				String soxNode,
				String soxJid,
				String soxPassword,
				boolean isUsingDefaultUser,
				boolean isExisting,
				boolean isRecordStopped,
				boolean isSubscribed,
				Timestamp created,
				boolean isSubscribeFailed,
				double recentMonthlyAverageDataArrival,
				long recentMonthlyTotalDataArrival,
				long recentMonthlyDataArrivalDays
			) {
		this.databaseId = databaseId;
		this.soxServer = soxServer;
		this.soxNode = soxNode;
		this.soxJid = soxJid;
		this.soxPassword = soxPassword;
		this.isUsingDefaultUser = isUsingDefaultUser;
		this.isExisting = isExisting;
		this.isRecordStopped = isRecordStopped;
		this.isSubscribed = isSubscribed;
		this.created = created;
		this.isSubscribeFailed = isSubscribeFailed;
		this.recentMonthlyAverageDataArrival = recentMonthlyAverageDataArrival;
		this.recentMonthlyTotalDataArrival = recentMonthlyTotalDataArrival;
		this.recentMonthlyDataArrivalDays = recentMonthlyDataArrivalDays;
	}

	public long getDatabaseId() {
		return databaseId;
	}

	public String getSoxServer() {
		return soxServer;
	}

	public String getSoxNode() {
		return soxNode;
	}

	public String getSoxJid() {
		return soxJid;
	}

	public String getSoxPassword() {
		return soxPassword;
	}

	public boolean isUsingDefaultUser() {
		return isUsingDefaultUser;
	}

	public boolean isExisting() {
		return isExisting;
	}

	public boolean isRecordStopped() {
		return isRecordStopped;
	}

	public boolean isSubscribed() {
		return isSubscribed;
	}

	public Timestamp getCreated() {
		return created;
	}

	public boolean isSubscribeFailed() {
		return isSubscribeFailed;
	}

	public double getRecentMonthlyAverageDataArrival() {
		return recentMonthlyAverageDataArrival;
	}

	public long getRecentMonthlyTotalDataArrival() {
		return recentMonthlyTotalDataArrival;
	}

	public long getRecentMonthlyDataArrivalDays() {
		return recentMonthlyDataArrivalDays;
	}
	
	public NodeIdentifier getNodeId() {
		return new NodeIdentifier(soxServer, soxNode);
	}
	
	public SoxLoginInfo getSoxLoginInfo() {
		return new SoxLoginInfo(soxServer, soxJid, soxPassword);
	}

}
