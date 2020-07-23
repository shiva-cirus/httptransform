package io.cdap.plugin.http;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class HttpConfig extends PluginConfig {

  public static final String PROPETY_HTTPGET_URLFIELD = "httpURLField";
  public static final String PROPERTY_HTTP_METHOD = "httpMethod";
  public static final String PROPERTY_HEADERS = "headers";
  public static final String PROPERTY_REQUEST_BODY = "requestBody";
  public static final String PROPERTY_RESPONSE_FIELD = "responseField";
  public static final String PROPERTY_USERNAME = "username";
  public static final String PROPERTY_PASSWORD = "password";
  public static final String PROPERTY_AUTH_TOKEN = "authToken";
  public static final String PROPERTY_AUTH_LOOKUP = "authLookup";
  public static final String OUTPUTSCHEMA_FIELD = "schema";

  @Name(PROPETY_HTTPGET_URLFIELD)
  @Description("Field containing HTTP Get URL.")
  private String httpURLField;


  @Name(PROPERTY_HTTP_METHOD)
  @Description("HTTP request method.")
  @Macro
  protected String httpMethod;

  @Name(PROPERTY_HEADERS)
  @Nullable
  @Description("Headers to send with each HTTP request.")
  @Macro
  protected String headers;

  @Nullable
  @Name(PROPERTY_REQUEST_BODY)
  @Description("Field name containing the HTTP post data.")
  protected String requestBody;

  @Name(PROPERTY_RESPONSE_FIELD)
  @Description("Output schema field name to store the HTTP response.")
  protected String httpresponseField;


  @Nullable
  @Name(PROPERTY_USERNAME)
  @Description("To support Input record driven authentication, the username is a JSON containing mappings for each " +
    "marketing program. eg: { '291':'345f45', '415':'4fd56' }")
  @Macro
  protected String username;

  @Nullable
  @Name(PROPERTY_PASSWORD)
  @Description("To support Input record driven authentication, the password is a JSON containing mappings for " +
    "each marketing program. eg: { '291':'4d6se', '415':'guidwx' }" )
  @Macro
  protected String password;


  @Name(PROPERTY_AUTH_TOKEN)
  @Nullable
  @Description("To support Input record driven authentication, the auth token is a JSON containing mappings " +
    "for each marketing program. eg: { '291':'345f45', '415':'4fd56' }")
  @Macro
  protected String authToken;


  @Name(PROPERTY_AUTH_LOOKUP)
  @Nullable
  @Description("Input field used to Lookup the Authorization JSON field. for eg: Field containing Marketing Program Number ")
  protected String authLookup;


  @Name(OUTPUTSCHEMA_FIELD)
  @Description("Specifies the schema of the records outputted from this plugin.")
  private final String schema;


  public String getHttpgetUrlField() {
    return httpURLField;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  @Nullable
  public String getHeaders() {
    return headers;
  }

  @Nullable
  public String getRequestBody() {
    return requestBody;
  }

  public String getResponseField() {
    return httpresponseField;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public String getAuthToken() {
    return authToken;
  }

  public String getSchema() {
    return schema;
  }

  @Nullable
  public String getAuthLookup() {
    return authLookup;
  }

  public HttpConfig(String httpURLField, String schema) {
    this.httpURLField = httpURLField;
    this.schema = schema;
  }

  public void validate(Schema inputSchema) throws IllegalArgumentException {
    // It's usually a good idea to check the schema. Sometimes users edit
    // the JSON config directly and make mistakes.
    if ( inputSchema == null){
      throw new IllegalArgumentException("Input schema is unknown. Define schema");
    }

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

    if (username != null && !containsMacro(username)) {
          if (!isJson(username)){
            throw new IllegalArgumentException(
              String.format("Username is not a valid JSON. Refer to documentation for details." ));
          }
    }

    if (password != null && !containsMacro(password)) {
      if (!isJson(password)){
        throw new IllegalArgumentException(
          String.format("Password is not a valid JSON . Refer to documentation for details." ));
      }
    }

    if (authToken != null && !containsMacro(authToken)) {
      if (!isJson(authToken)){
        throw new IllegalArgumentException(
          String.format("Authorization token is not in valid JSON format. Refer to documentation for details." ));
      }
    }

    if  ( (authToken != null && !containsMacro(authToken)) || (password != null && !containsMacro(password)) ||
      (username != null && !containsMacro(username)) ){

      // Check if Lookup field is provided.

      if( authLookup == null){
        throw new IllegalArgumentException(
          String.format("Lookup field is required to pass the required security credentials." ));
      }

      Schema.Field authLookupField = inputSchema.getField(authLookup);
      if (authLookupField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in input schema %s.", authLookup, schema));
      }
      fieldSchema = authLookupField.getSchema();
      fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                        authLookup, fieldType, Schema.Type.STRING));
      }
    }

    if (schema == null){
      throw new IllegalArgumentException("Out schema is unknown. Define schema");
    }

    // This method should be used to validate that the configuration is valid.
    if (httpresponseField == null || httpresponseField.isEmpty()) {
      throw new IllegalArgumentException("Response Field is a required.");
    }

    try {
      Schema outputSchema = Schema.parseJson(schema);
      Schema.Field responseField = outputSchema.getField(httpresponseField);
      if (responseField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in output schema %s.", httpresponseField, schema));
      }
      fieldSchema = responseField.getSchema();
      fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                        httpresponseField, fieldType, Schema.Type.STRING));
      }

    } catch (IOException ex){
      throw new IllegalArgumentException("Outschema is unknown or not defined.");
    }
  }


  public static Map<String, String> getMapFromKeyValueString(String keyValueString) {
    Map<String, String> result = new LinkedHashMap<>();

    if (keyValueString == null || keyValueString.length() == 0) {
      return result;
    }

    String[] mappings = keyValueString.split(",");
    for (String map : mappings) {
      String[] columns = map.split(":");
      result.put(columns[0], columns[1]);
    }
    return result;
  }

  public static boolean isJson(String Json) {
    Gson gson = new Gson();
    try {
      gson.fromJson(Json, Object.class);
      Object jsonObjType = gson.fromJson(Json, Object.class).getClass();
      if(jsonObjType.equals(String.class)){
        return false;
      }
      return true;
    } catch (com.google.gson.JsonSyntaxException ex) {
      return false;
    }
  }



}
