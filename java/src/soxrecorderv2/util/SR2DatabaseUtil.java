package soxrecorderv2.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class SR2DatabaseUtil {
	
	public static final String SQL_GET_BLACKLIST = "SELECT sox_node FROM blacklist WHERE sox_server = ?;";
	
	public static Set<String> getBlacklistNodes(Connection conn, String soxServer) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(SQL_GET_BLACKLIST);
		ps.setString(1, soxServer);
		ResultSet rs = ps.executeQuery();
		Set<String> ret = new HashSet<>();
		while (rs.next()) {
			ret.add(rs.getString(1));
		}
		rs.close();
		ps.close();
		return ret;
	}

}
