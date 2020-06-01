# HttpGet Transform

Description
-----------

Transform to invoke HTTP using Get method from input field and return back response JSON.
The HTTP method should only return a single JSON object. Lists are not supported.

Use Case
--------

This transform is used to enrich / validate from an external HTTP endpoint. The output of the HTTP Get is a JSON object.

Properties
----------

**httpURLField:** Comma separated list of fields to uppercase.

Example
-------

{
        "name": "HttpGet",
        "plugin": {
          "name": "HttpGet",
          "type": "transform",
          "label": "HttpGet",
          "artifact": {
            "name": "httptransform",
            "version": "1.0-SNAPSHOT",
            "scope": "USER"
          },
          "properties": {
            "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"url\",\"type\":[\"string\",\"null\"]}]}",
            "httpURLField": "url"
          }
        }
      }
