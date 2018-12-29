package template.CommHttpUtils;

import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class HttpClientUtils {

	public static final String CHARSET = "UTF-8";

	public static HttpClientResult doPost(String url) throws Exception {
		return doPost(url, null, null);
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
		return doPost(url, headers, params, CHARSET);
	}

	public static HttpClientResult doPost(String url, Map<String, String> headers, Map<String, String> params, String charset) throws Exception {

		PostMethod postMethod = new PostMethod(url);

		try {
			postMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, charset);

			packageHeader(headers, postMethod);

			if (params != null) {
				Set<Map.Entry<String, String>> entrySet = params.entrySet();
				for (Map.Entry<String, String> entry : entrySet) {
					postMethod.addParameter(entry.getKey(), entry.getValue());
				}
			}

			HttpClient httpClient = new HttpClient();
			HttpConnectionManagerParams managerParams = httpClient.getHttpConnectionManager().getParams();
			managerParams.setConnectionTimeout(60000);
			managerParams.setSoTimeout(120000);
			httpClient.executeMethod(postMethod);
			int code = postMethod.getStatusCode();
			if (code == HttpStatus.SC_OK) {
				String content = postMethod.getResponseBodyAsString();
				if (content != null)
					content = content.trim();

				return new HttpClientResult(postMethod.getStatusCode(), content);
			}

			return new HttpClientResult(HttpStatus.SC_INTERNAL_SERVER_ERROR);
		} finally {
			if (postMethod != null) {
				postMethod.releaseConnection();
			}
		}
	}

	/**
	 * Description: 封装请求头
	 */
	public static void packageHeader(Map<String, String> params, PostMethod httpMethod) {
		// 封装请求头
		if (params != null) {
			Set<Map.Entry<String, String>> entrySet = params.entrySet();
			for (Map.Entry<String, String> entry : entrySet) {

				Header header = new Header(entry.getKey(), entry.getValue());

				httpMethod.addRequestHeader(header);
			}
		}
	}
}
