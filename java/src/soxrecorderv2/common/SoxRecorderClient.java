package soxrecorderv2.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import net.arnx.jsonic.JSON;
import soxrecorderv2.common.exception.SR2CommunicationException;
import soxrecorderv2.common.model.ExportingState;
import soxrecorderv2.common.model.ObConnInfo;

/**
 * client library to call SoxRecorderV2 HTTP Controller WebAPI
 * @author tomotaka
 *
 */
public class SoxRecorderClient {
	
	public enum HttpMethod {
		GET, POST;
	}
	
//	public static final String DEFAULT_BASE_URL = "http://localhost:43280/";
	
	public static final String API_PATH_SERVER_LIST = "/api/server/list";
	public static final String API_PATH_ADD_OBSERVATION = "/api/observation/add";
	public static final String API_PATH_UPDATE_EXPORT_STATE = "/api/export/update-state/<export_id>";
	
	public static final String PROP_KEY_ENDPOINT = "endpoint";
	public static final String PROP_KEY_API_KEY = "apikey";
	
	public static SoxRecorderClient buildFromProperties(Properties config) {
		String endpoint = config.getProperty(PROP_KEY_ENDPOINT);
		String apiKey = config.getProperty(PROP_KEY_API_KEY);
		return new SoxRecorderClient(endpoint, apiKey);
	}
	
	private final String endpoint;
	private final String apiKey;
	
	public SoxRecorderClient(String endpoint, String apiKey) {
		this.endpoint = endpoint;
		this.apiKey = apiKey;
	}
	
	/**
	 * query controller to ask if the apiKey is valid or not.
	 * 
	 * @return true if apiKey is valid, false if invalid.
	 */
	public boolean checkKeyValidity() {
		return false;  // TODO
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	
	public String getApiKey() {
		return apiKey;
	}
	
	public boolean addObservation(ObConnInfo obConnInfo, String node) throws SR2CommunicationException {
		String apiEndpoint = buildApiEndpoint(API_PATH_ADD_OBSERVATION);

		String soxServer = obConnInfo.getServer();
		String soxJid = obConnInfo.getJid();
		String soxPassword = obConnInfo.getPassword();
		boolean isAnonymous = (soxJid == null && soxPassword == null);
		
		Map<String, String> params = new HashMap<>();
		params.put("api_key", getApiKey());
		params.put("sox_server", soxServer);
		params.put("sox_jid", soxJid);
		params.put("sox_password", soxPassword);
		params.put("sox_node", node);
		params.put("is_anonymous", (isAnonymous) ? "true" : "false");
		
		Map<String, Object> result = null;
		try {
			result = performHttpPost(apiEndpoint, params);
		} catch (URISyntaxException | IOException | SR2InvalidApiKeyException e) {
			// TODO logging
			throw new SR2CommunicationException(e.toString(), e);
		}
		
		String status = (String)result.get("status");
		
		return status.equals("ok"); 
	}
	
	@SuppressWarnings("unchecked")
	public List<String> getSoxServerList() throws SR2CommunicationException, SR2InvalidApiKeyException {
		String apiEndpoint = buildApiEndpoint(API_PATH_SERVER_LIST);
	
		Map<String, String> params = new HashMap<>();
		params.put("api_key", getApiKey());

		Map<String, Object> result = null;
		
		try {
			result = performHttpPost(apiEndpoint, params);
		} catch (URISyntaxException | IOException e) {
			// TODO logging
			throw new SR2CommunicationException(e.toString(), e);
		}
		
		Map<String, Object> resultData = (Map<String, Object>)result.get("result");
		List<String> servers = (List<String>)resultData.get("result");
		
		return servers;
	}
	
	public boolean updateExportState(long exportId, ExportingState newState) throws SR2CommunicationException, SR2InvalidApiKeyException {
		String apiEndpoint = buildApiEndpoint(API_PATH_UPDATE_EXPORT_STATE);
		
		apiEndpoint = apiEndpoint.replace("<export_id>", Long.toString(exportId));
		
		Map<String, String> params = new HashMap<>();
		params.put("api_key", getApiKey());
		params.put("state", Integer.toString(newState.getState()));
		
		Map<String, Object> result = null;
		try {
			result = performHttpPost(apiEndpoint, params);
		} catch (URISyntaxException | IOException e) {
			// TODO: logging
			throw new SR2CommunicationException(e.toString(), e);
		}
		
		String status = (String)result.get("status");
		
		return status.equals("ok");
	}
	
	private String buildApiEndpoint(String path) {
		StringBuilder sb = new StringBuilder(endpoint);
		if (!endpoint.endsWith("/")) {
			sb.append("/");
		}

		if (path.startsWith("/")) {
			sb.append(path.substring(1));
		} else {
			sb.append(path);
		}

		return sb.toString();
	}
	
	/**
	 * according to: http://qiita.com/mychaelstyle/items/e02b3011d1e71bfa26c5
	 * 
	 * http component javadoc: https://hc.apache.org/httpcomponents-client-4.5.x/httpclient/apidocs/
	 * 
	 * @param url
	 * @param params
	 * @return
	 * @throws URISyntaxException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws SR2InvalidApiKeyException 
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> performHttpPost(String url, Map<String, String> params)
			throws URISyntaxException, ClientProtocolException, IOException, SR2InvalidApiKeyException {
		URIBuilder builder = new URIBuilder(url);
		URI uri = builder.build();
		
		HttpPost method = new HttpPost(uri);
		
		List<NameValuePair> pairs = new ArrayList<>();
		for (String k : params.keySet()) {
			pairs.add(new BasicNameValuePair(k, params.get(k)));
		}
		method.setEntity(new UrlEncodedFormEntity(pairs, "UTF-8"));
		
		HttpClient httpClient = new DefaultHttpClient();
		HttpResponse response = httpClient.execute(method);
		if (response.getStatusLine().getStatusCode() == 400) {
			throw new SR2InvalidApiKeyException();
		}
		String body = EntityUtils.toString(response.getEntity(), "UTF-8");
		
		return (Map<String, Object>)JSON.decode(body);
	}
	
	@SuppressWarnings({ "unchecked", "unused" })
	private Map<String, Object> performHttpGet(String url, Map<String, String> params)
			throws URISyntaxException, ClientProtocolException, IOException, SR2InvalidApiKeyException {
		URIBuilder builder = new URIBuilder(url);
		for (String k : params.keySet()) {
			builder.addParameter(k, params.get(k));
		}
		
		URI uri = builder.build();
		
		HttpGet method = new HttpGet(uri);
		
		HttpClient httpClient = new DefaultHttpClient();
		HttpResponse response = httpClient.execute(method);
		if (response.getStatusLine().getStatusCode() == 400) {
			throw new SR2InvalidApiKeyException();
		}
		String body = EntityUtils.toString(response.getEntity(), "UTF-8");
		
		return (Map<String, Object>)JSON.decode(body);
	}
	
//	private Map<String, Object> performHttpAccess(HttpMethod method, String url, Map<String, String> params) {
//		// TODO
//		return null;
//	}

}
