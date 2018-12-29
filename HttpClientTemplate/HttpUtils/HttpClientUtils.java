package template.HttpUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * Description: httpClient Util
 */
public class HttpClientUtils {

	// Default Encoding: UTF-8
	private static final String ENCODING = "UTF-8";

	// 设置连接超时时间，单位毫秒。
	private static final int CONNECT_TIMEOUT = 10000;

	// 请求获取数据的超时时间(即响应时间)，单位毫秒。
	private static final int SOCKET_TIMEOUT = 60000;

	/**
	 * 发送get请求；不带请求头和请求参数
	 */
	public static HttpClientResult doGet(String url) throws Exception {
		return doGet(url, null, null);
	}

	/**
	 * 发送get请求；带请求参数
	 */
	public static HttpClientResult doGet(String url, Map<String, String> params) throws Exception {
		return doGet(url, null, params);
	}

	/**
	 * 发送get请求；带请求头和请求参数
	 */
	public static HttpClientResult doGet(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
		// 创建httpClient对象
		CloseableHttpClient httpClient = HttpClients.createDefault();

		// 创建访问的地址
		URIBuilder uriBuilder = new URIBuilder(url);
		if (params != null) {
			Set<Entry<String, String>> entrySet = params.entrySet();
			for (Entry<String, String> entry : entrySet) {
				uriBuilder.setParameter(entry.getKey(), entry.getValue());
			}
		}

		// 创建http对象
		HttpGet httpGet = new HttpGet(uriBuilder.build());
		/**
		 * setConnectTimeout：设置连接超时时间，单位毫秒。
		 * setConnectionRequestTimeout：设置从connect Manager(连接池)获取Connection
		 * 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
		 * setSocketTimeout：请求获取数据的超时时间(即响应时间)，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
		 */
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
		httpGet.setConfig(requestConfig);

		// 设置请求头
		packageHeader(headers, httpGet);

		// 创建httpResponse对象
		CloseableHttpResponse httpResponse = null;

		try {
			// 执行请求并获得响应结果
			return getHttpClientResult(httpResponse, httpClient, httpGet);
		} finally {
			// 释放资源
			release(httpResponse, httpClient);
		}
	}

	public static HttpClientResult doPost(String url) throws Exception {
		return doPost(url, null, null);
	}

	public static HttpClientResult doPost(String url, Map<String, String> params) throws Exception {
		return doPost(url, null, params);
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
		return doPost(url, headers, params, null, ENCODING, null);
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params,
	                                      String body) throws Exception {
		return doPost(url, headers, params, body, null, ContentType.TEXT_PLAIN);
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params,
	                                      String body, String encoding) throws Exception {
		return doPost(url, headers, params, body, encoding, null);
	}

	public static HttpClientResult doPostJson(String url, Map<String, String> headers, Map<String, String> params,
	                                          String body) throws Exception {
		return doPost(url, headers, params, body, null, ContentType.APPLICATION_JSON);
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params,
	                                      Map<String, String> body) throws Exception {
		return doPost(url, headers, params, body, ENCODING);
	}

	/**
	 * 发送post请求；带请求头和请求参数
	 */
	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params,
	                                      Map<String, String> body, String encoding) throws Exception {
		// 创建httpClient对象
		CloseableHttpClient httpClient = HttpClients.createDefault();

		// 创建访问的地址
		URIBuilder uriBuilder = new URIBuilder(url);
		if (params != null) {
			Set<Entry<String, String>> entrySet = params.entrySet();
			for (Entry<String, String> entry : entrySet) {
				uriBuilder.setParameter(entry.getKey(), entry.getValue());
			}
		}

		// 创建http对象
		HttpPost httpPost = new HttpPost(uriBuilder.build());
		/**
		 * setConnectTimeout：设置连接超时时间，单位毫秒。
		 * setConnectionRequestTimeout：设置从connect Manager(连接池)获取Connection
		 * 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
		 * setSocketTimeout：请求获取数据的超时时间(即响应时间)，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
		 */
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
		httpPost.setConfig(requestConfig);

		// 设置请求头
		/*httpPost.setHeader("Cookie", "");
		httpPost.setHeader("Connection", "keep-alive");
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Accept-Language", "zh-CN,zh;q=0.9");
		httpPost.setHeader("Accept-Encoding", "gzip, deflate, br");
		httpPost.setHeader("Content-Type", "text/html; charset=utf-8");
		httpPost.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36");*/
		packageHeader(headers, httpPost);

		// 封装请求参数
		packageParam(body, httpPost, encoding);

		// 创建httpResponse对象
		CloseableHttpResponse httpResponse = null;

		try {
			// 执行请求并获得响应结果
			return getHttpClientResult(httpResponse, httpClient, httpPost, encoding);
		} finally {
			// 释放资源
			release(httpResponse, httpClient);
		}
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params,
	                                      String body, String encoding, ContentType ctype) throws Exception {
		// 创建httpClient对象
		CloseableHttpClient httpClient = HttpClients.createDefault();

		// 创建访问的地址
		URIBuilder uriBuilder = new URIBuilder(url);
		if (params != null) {
			Set<Entry<String, String>> entrySet = params.entrySet();
			for (Entry<String, String> entry : entrySet) {
				uriBuilder.setParameter(entry.getKey(), entry.getValue());
			}
		}

		// 创建http对象
		HttpPost httpPost = new HttpPost(uriBuilder.build());
		/**
		 * setConnectTimeout：设置连接超时时间，单位毫秒。
		 * setConnectionRequestTimeout：设置从connect Manager(连接池)获取Connection
		 * 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
		 * setSocketTimeout：请求获取数据的超时时间(即响应时间)，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
		 */
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
		httpPost.setConfig(requestConfig);

		// 设置请求头
		packageHeader(headers, httpPost);

		// 封装请求参数
		if (ctype != null) {
			packageParam(body, httpPost, ctype);
		} else {
			packageParam(body, httpPost, encoding);
		}

		// 创建httpResponse对象
		CloseableHttpResponse httpResponse = null;

		try {
			// 执行请求并获得响应结果
			return getHttpClientResult(httpResponse, httpClient, httpPost, encoding);
		} finally {
			// 释放资源
			release(httpResponse, httpClient);
		}
	}

	/**
	 * 发送put请求；不带请求参数
	 */
	public static HttpClientResult doPut(String url) throws Exception {
		return doPut(url);
	}

	/**
	 * 发送put请求；带请求参数
	 */
	public static HttpClientResult doPut(String url, Map<String, String> params) throws Exception {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpPut httpPut = new HttpPut(url);
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
		httpPut.setConfig(requestConfig);

		packageParam(params, httpPut);

		CloseableHttpResponse httpResponse = null;

		try {
			return getHttpClientResult(httpResponse, httpClient, httpPut);
		} finally {
			release(httpResponse, httpClient);
		}
	}

	/**
	 * 发送delete请求；不带请求参数
	 */
	public static HttpClientResult doDelete(String url) throws Exception {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpDelete httpDelete = new HttpDelete(url);
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
		httpDelete.setConfig(requestConfig);

		CloseableHttpResponse httpResponse = null;
		try {
			return getHttpClientResult(httpResponse, httpClient, httpDelete);
		} finally {
			release(httpResponse, httpClient);
		}
	}

	/**
	 * 发送delete请求；带请求参数
	 */
	public static HttpClientResult doDelete(String url, Map<String, String> params) throws Exception {
		if (params == null) {
			params = new HashMap<String, String>();
		}

		params.put("_method", "delete");
		return doPost(url, params);
	}

	/**
	 * Description: 封装请求头
	 */
	public static void packageHeader(Map<String, String> params, HttpRequestBase httpMethod) {
		// 封装请求头
		if (params != null) {
			Set<Entry<String, String>> entrySet = params.entrySet();
			for (Entry<String, String> entry : entrySet) {
				// 设置到请求头到HttpRequestBase对象中
				httpMethod.setHeader(entry.getKey(), entry.getValue());
			}
		}
	}

	public static void packageParam(Map<String, String> params, HttpEntityEnclosingRequestBase httpMethod)
			throws UnsupportedEncodingException {
		packageParam(params, httpMethod, ENCODING);
	}

	/**
	 * Description: 封装请求参数
	 */
	public static void packageParam(Map<String, String> params, HttpEntityEnclosingRequestBase httpMethod,
	                                String encoding)
			throws UnsupportedEncodingException {
		// 封装请求参数
		if (params != null) {
			List<NameValuePair> nvps = new ArrayList<NameValuePair>();
			Set<Entry<String, String>> entrySet = params.entrySet();
			for (Entry<String, String> entry : entrySet) {
				nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
			}

			// 设置到请求的http对象中
			httpMethod.setEntity(new UrlEncodedFormEntity(nvps, encoding));
		}
	}

	public static void packageParam(String params, HttpEntityEnclosingRequestBase httpMethod)
			throws UnsupportedCharsetException {
		packageParam(params, httpMethod, ENCODING);
	}

	public static void packageParam(String params, HttpEntityEnclosingRequestBase httpMethod,
	                                String encoding)
			throws UnsupportedCharsetException {

		// 封装请求参数
		if (params != null) {
			// 设置到请求的http对象中
			httpMethod.setEntity(new StringEntity(params, encoding));
		}
	}

	public static void packageParam(String params, HttpEntityEnclosingRequestBase httpMethod,
	                                ContentType ctype)
			throws UnsupportedCharsetException {

		// 封装请求参数
		if (params != null) {
			// 设置到请求的http对象中
			httpMethod.setEntity(new StringEntity(params, ctype));
		}
	}

	/**
	 * Description: 获得响应结果
	 */
	public static HttpClientResult getHttpClientResult(CloseableHttpResponse httpResponse,
	                                                   CloseableHttpClient httpClient, HttpRequestBase httpMethod,
	                                                   String encoding) throws Exception {
		// 执行请求
		httpResponse = httpClient.execute(httpMethod);

		// 获取返回结果
		if (httpResponse != null && httpResponse.getStatusLine() != null) {
			String content = "";
			if (httpResponse.getEntity() != null) {
				content = EntityUtils.toString(httpResponse.getEntity(), encoding);
			}
			return new HttpClientResult(httpResponse.getStatusLine().getStatusCode(), content);
		}
		return new HttpClientResult(HttpStatus.SC_INTERNAL_SERVER_ERROR);
	}

	public static HttpClientResult getHttpClientResult(CloseableHttpResponse httpResponse,
	                                                   CloseableHttpClient httpClient, HttpRequestBase httpMethod) throws Exception {
		return getHttpClientResult(httpResponse, httpClient, httpMethod, ENCODING);
	}

	/**
	 * Description: 释放资源
	 */
	public static void release(CloseableHttpResponse httpResponse, CloseableHttpClient httpClient) throws IOException {
		// 释放资源
		if (httpResponse != null) {
			httpResponse.close();
		}
		if (httpClient != null) {
			httpClient.close();
		}
	}

}