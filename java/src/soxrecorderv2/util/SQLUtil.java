package soxrecorderv2.util;

import java.sql.Timestamp;
import java.util.Calendar;

public class SQLUtil {

	public static String buildInsertSql(String tableName, String[] fields) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append("(");
		
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(fields[i]);
		}
		
		sb.append(") VALUES (");
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("?");
		}
		sb.append(");");
		
		return sb.toString();
	}
	
	/**
	 * n=3だったら "(?,?,?)" みたいなプレースホルダのリストをつくる
	 * @param n
	 * @return
	 */
	public static String buildPlaceholders(int n) {
		StringBuilder sb = new StringBuilder("(");
		for (int i = 0; i < n; i++) {
			if (0 < i) {
				sb.append(',');
			}
			sb.append('?');
		}
		sb.append(')');
		return sb.toString();
	}
	
	public static Timestamp getCurrentTimestamp() {
		Calendar calendar = Calendar.getInstance();
		java.util.Date nowDate = calendar.getTime();
		java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(nowDate.getTime());
		return currentTimestamp;
	}

}
