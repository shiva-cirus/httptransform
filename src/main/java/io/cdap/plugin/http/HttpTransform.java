/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.http;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 * For full documentation, check out:
 * https://docs.cask.co/cdap/current/en/developer-manual/pipelines/developing-plugins/index.html
 */

@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Http")
@Description("Transforms configured fields to lowercase or uppercase.")
public class HttpTransform extends Transform<StructuredRecord, StructuredRecord> {

  // If you want to log things, you will need this line
  private static final Logger LOG = LoggerFactory.getLogger(HttpTransform.class);


  // Usually, you will need a private variable to store the config that was passed to your class
  private final HttpConfig config;
  private Schema outputSchema;
  private String httpURLField;
  private Gson gson;

  /**
   * Config properties for the plugin.
   */


  public HttpTransform(HttpConfig config) {
    this.config = config;
  }

  /**
   * This function is called when the pipeline is published. You should use this for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer(). Those parameters will be stored and will be made
   * available to your plugin during runtime via the TransformContext. Any errors thrown here will stop the pipeline
   * from being published.
   *
   * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets and streams and storing parameters
   * @throws IllegalArgumentException If the config is invalid.
   */

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    // the output schema is always the same as the input schema
    Schema inputSchema = stageConfigurer.getInputSchema();
    config.validate(inputSchema);

    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.getSchema()));
    } catch (IOException e) {
      throw new IllegalArgumentException("Output schema cannot be parsed.", e);
    }
  }

  // initialize is called once at the start of each pipeline run
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    httpURLField = config.getHttpgetUrlField();
    outputSchema = Schema.parseJson(config.getSchema());
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    String url = getURL(input);
    String userName = getUserName(input);
    String pw = getPassword(input);
    Map<String, String> authHeaders = getAuthHeader(input);
    Map<String, String> httpHeaders = config.getMapFromKeyValueString(config.getHeaders());

    String responseBody = invokeHttp(url, userName, pw, authHeaders, httpHeaders);
    builder.set(config.getResponseField(),responseBody);

    List<Schema.Field> fields = outputSchema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (input.get(name) != null) {
        builder.set(name, input.get(name));
      }
    }

    emitter.emit(builder.build());
  }

  private String getURL(StructuredRecord input) throws Exception {
    //Check if input record contains the HTTP URL Get URL
    if (input.get(httpURLField) == null) {
      new Exception("Input Record does not contain field " + httpURLField);
    }
    return input.get(httpURLField);
  }

  @Nullable
  private String getUserName(StructuredRecord input) throws Exception {
    if (config.getUsername() == null) {
      return null;
    }
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(config.getUsername(), Map.class);
    String lookup = input.get(config.getAuthLookup());
    if (!map.containsKey(lookup)) {
      new Exception("Unable to find Username for " + lookup);

    }
    return map.get(lookup);
  }

  @Nullable
  private String getPassword(StructuredRecord input) throws Exception {
    if (config.getPassword() == null) {
      return null;
    }
    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(config.getPassword(), Map.class);
    String lookup = input.get(config.getAuthLookup());
    if (!map.containsKey(lookup)) {
      new Exception("Unable to find Password for " + lookup);
    }
    return map.get(lookup);
  }

  @Nullable
  private Map<String, String> getAuthHeader(StructuredRecord input) throws Exception {
    if (config.getAuthToken() == null) {
      return null;
    }

    Gson gson = new Gson();
    Map<String, String> map = gson.fromJson(config.getAuthToken(), Map.class);
    String lookup = input.get(config.getAuthLookup());
    if (!map.containsKey(lookup)) {
      new Exception("Unable to find Authtoken for " + lookup);
    }
    Map<String,String> hdr = new HashMap<String, String>();
    hdr.put("Authorization",map.get(lookup));
    return hdr;
  }


  private String invokeHttp(String url, String userName, String password, Map<String, String> authHeaders, Map<String, String> httpHeaders) throws IOException {

    CloseableHttpClient httpclient = null;
    try {
      httpclient = HttpClient.getInstance(url, userName, password, authHeaders, httpHeaders);
      HttpGet get = new HttpGet(url);
      HttpResponse response = httpclient.execute(get);
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != 200) {
        //throw new IOException("Failed invoking URL - " + url + " with HTTP error code : " + statusCode);
        String error = "{ \"httperror\": { \"code\": \"" + statusCode + "\", \"message\": \"Error Invoking URL "+ url +" \"} }";
        return error;
      }
      HttpEntity httpEntity = response.getEntity();
      byte[] bytes = bytes = EntityUtils.toByteArray(httpEntity);
      String body = new String(bytes, StandardCharsets.UTF_8);
      return body;


    } catch (Exception ex){
      String error = "{ \"httperror\": { \"code\": \"" + 500 + "\", \"message\": \"Error Invoking URL "+ url +" " + ex.getLocalizedMessage() +" \"} }";
      return error;
    }finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }


}
