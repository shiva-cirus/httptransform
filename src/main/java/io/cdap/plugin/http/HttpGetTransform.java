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
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 * For full documentation, check out: https://docs.cask.co/cdap/current/en/developer-manual/pipelines/developing-plugins/index.html
 */

@Plugin(type = Transform.PLUGIN_TYPE)
@Name("HttpGet")
@Description("Transforms configured fields to lowercase or uppercase.")
public class HttpGetTransform extends Transform<StructuredRecord, StructuredRecord> {

  // If you want to log things, you will need this line
  private static final Logger LOG = LoggerFactory.getLogger(HttpGetTransform.class);


  // Usually, you will need a private variable to store the config that was passed to your class
  private final Conf config;
  private Schema outputSchema;
  private String httpURLField;
  private Gson gson;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    public static final String HTTPGET_URL_FIELD = "httpURLField";
    public static final String OUTPUTSCHEMA_FIELD = "schema";

    @Name(HTTPGET_URL_FIELD)
    @Description("Field containing HTTP Get URL.")
    private String httpURLField;

    @Name(OUTPUTSCHEMA_FIELD)
    @Description("Specifies the schema of the records outputted from this plugin.")
    private final String schema;


    private String getHttpgetUrlField() {
      return httpURLField;
    }

    public Conf(String httpURLField, String schema) {
      this.httpURLField = httpURLField;
      this.schema = schema;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      // It's usually a good idea to check the schema. Sometimes users edit
      // the JSON config directly and make mistakes.
      try {
        Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Output schema cannot be parsed.", e);
      }
      // This method should be used to validate that the configuration is valid.
      if (httpURLField == null || httpURLField.isEmpty()) {
        throw new IllegalArgumentException("httpURLField is a required field.");
      }

      Schema.Field inputField = inputSchema.getField(httpURLField);
      if (inputField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in input schema %s.", httpURLField, schema));
      }
      Schema fieldSchema = inputField.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                        httpURLField, fieldType, Schema.Type.STRING));
      }

      // You can use the containsMacro() function to determine if you can validate at deploy time or runtime.
      // If your plugin depends on fields from the input schema being present or the right type, use inputSchema
    }

  }

  public HttpGetTransform(Conf config) {
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

    // if schema is null, that means it is either not known until runtime, or it is variable
    if (inputSchema != null) {
      // if the input schema is constant and known at configure time, check that all configured fields are strings
      config.validate(inputSchema);
    }

    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Output schema cannot be parsed.", e);
    }

  }

  // initialize is called once at the start of each pipeline run
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    httpURLField = config.getHttpgetUrlField();
    outputSchema = Schema.parseJson(config.schema);
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    //Check if input record contains the HTTP URL Get URL
    if (input.get(httpURLField) == null) {
      new Exception("Input Record does not contain field " + httpURLField);
    }

    String url = input.get(httpURLField);
    Map<String, Object> result = invokeHttp(url);

    List<Schema.Field> fields = outputSchema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (input.get(name) != null){
        builder.set(name,input.get(name));
      }

      if (result.get(name) != null) {
        builder.set(name, result.get(name));
      }
    }

    emitter.emit(builder.build());

  }


  private Map<String, Object> invokeHttp(String url) throws IOException {
    CloseableHttpClient client = null;
    try {
      CloseableHttpClient httpclient = HttpClients.createDefault();
      HttpGet get = new HttpGet(url);
      HttpResponse response = httpclient.execute(get);
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != 200) {
        throw new IOException("Failed invoking URL - " + url + " with HTTP error code : " + statusCode);
      }
      HttpEntity httpEntity = response.getEntity();
      Gson gson = new GsonBuilder().create();
      Reader reader = new InputStreamReader(httpEntity.getContent(), Charset.forName("UTF-8"));
      return gson.fromJson(reader, new TypeToken<Map<String, Object>>() {
      }.getType());


    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

}
