package soxrecorderv2.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;

public class SQLUtil {
	
	private static final Joiner AND_JOINER = Joiner.on(" AND ");
	private static final Joiner OR_JOINER = Joiner.on(" OR ");

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
	
	public static Timestamp toTimestamp(Calendar cal) {
		java.util.Date date = cal.getTime();
		java.sql.Timestamp timestamp = new java.sql.Timestamp(date.getTime());
		return timestamp;
	}
	
	public static final String andJoin(List<String> conditions) {
		return AND_JOINER.join(addParen(conditions));
	}
	
	public static final String orJoin(List<String> conditions) {
		return OR_JOINER.join(addParen(conditions));
	}
	
	public static final List<String> addParen(List<String> conditions) {
		List<String> ret = new ArrayList<>();
		for (String cond : conditions) {
			if (!(cond.startsWith("(") && cond.endsWith(")"))) {
				ret.add("(" + cond + ")");
			} else {
				ret.add(cond);
			}
		}
		return Collections.unmodifiableList(ret);
	}

}
