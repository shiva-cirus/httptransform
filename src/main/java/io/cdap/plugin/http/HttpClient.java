package io.cdap.plugin.http;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpClient implements Closeable {

  private HttpClient(){

  }

  private CloseableHttpClient createHttpClient(long connectionTimeout, long readTimeout , String userName, String pw,  String url , Map<String,String> headers , Map<String,String> authHeaders) throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    // set timeouts
    Long connectTimeoutMillis = TimeUnit.SECONDS.toMillis(connectionTimeout);
    Long readTimeoutMillis = TimeUnit.SECONDS.toMillis(readTimeout);
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder.setSocketTimeout(readTimeoutMillis.intValue());
    requestBuilder.setConnectTimeout(connectTimeoutMillis.intValue());
    requestBuilder.setConnectionRequestTimeout(connectTimeoutMillis.intValue());
    httpClientBuilder.setDefaultRequestConfig(requestBuilder.build());

    // basic auth
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    URL aURL = new URL(url);
    if (userName != null) {
      AuthScope authScope = new AuthScope(HttpHost.create(aURL.getHost()));
      credentialsProvider.setCredentials(authScope,
                                         new UsernamePasswordCredentials(userName, pw));
    }

    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

    ArrayList<Header> clientHeaders = new ArrayList<>();

    if (authHeaders != null) {
      for (Map.Entry<String, String> headerEntry : authHeaders.entrySet())  {
        clientHeaders.add(new BasicHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }

    // set default headers
    if (headers != null) {
      for (Map.Entry<String, String> headerEntry : headers.entrySet())  {
        clientHeaders.add(new BasicHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }
    httpClientBuilder.setDefaultHeaders(clientHeaders);

    return httpClientBuilder.build();
  }

  public static CloseableHttpClient getInstance(String url, String userName, String pw, Map<String,String> authHeaders, Map<String,String> headers) throws IOException{

    return new HttpClient().createHttpClient(60,60,userName,pw,url,headers,authHeaders);

  }



  @Override
  public void close() throws IOException {

  }
}
