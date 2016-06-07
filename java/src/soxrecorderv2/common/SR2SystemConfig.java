package soxrecorderv2.common;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.arnx.jsonic.JSON;
import net.arnx.jsonic.JSONException;

public class SR2SystemConfig {

	private Map<String, String> config;
	private String file;
	
	public SR2SystemConfig(final String file) throws JSONException, IOException {
		File f = new File(file);
		
		if (!f.exists()) {
			throw new FileNotFoundException("not found: " + file);
		}
		
		this.file = file;
		this.config = new HashMap<>();
		
		reload();
	}
	
	@SuppressWarnings("unchecked")
	public void reload() throws JSONException, IOException {
		FileInputStream fin = new FileInputStream(file);
		BufferedInputStream in = new BufferedInputStream(fin);
		config = (Map<String, String>)JSON.decode(in);
	}
	
	public String getFile() {
		return file;
	}
	
	public String get(String key) {
		return config.get(config);
	}

}
