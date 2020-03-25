package net.ogalab.datacell.container.rdb;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.HashMap;

public class PreparedStatementPool {

	HashMap<String, PreparedStatement> pool = new HashMap<String, PreparedStatement>();
//	ArrayList<String> keys = new ArrayList<String>();
//	int size = 10;
	
	public PreparedStatement get(String key) {
		if (pool.containsKey(key)) {
			return pool.get(key);
		}
		else
			return null;
			
	}
	
	public void put(String key, PreparedStatement value) {
		if (!pool.containsKey(key)) {
			pool.put(key, value);
		}
	}
	
	public Collection<PreparedStatement> getAll() {
		return pool.values();
	}
	
}
