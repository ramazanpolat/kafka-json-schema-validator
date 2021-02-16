# kafka-json-schema-validator
Validates and filters Apache Kafka messages based on JSON Schema Draft-07

## What it does?
`kafka-json-schema-validator` is an Apache Kafka Stream that validates incoming messages to a topic using [everit-org JSON Schema Validator](https://github.com/everit-org/json-schema), then forwards validated message to an output topic.
If provided, it also forwards validation errors to another topic.

## Motivation
Installing and configuring Schema Registry is boring and takes time.

## Installation
```sh
$ mvn package assembly:single
```

## Usage

```sh
$ java -jar kafka-json-schema-validator-jar-with-dependencies.jar
Missing required options: s, i, o
usage: java -jar JsonValidatorStream.jar
 -e,--error <arg>    error(invalid) topic [not required]
 -i,--input <arg>    input topic
 -o,--output <arg>   output topic
 -p,--properties     properties (default='stream.properties')
 -s,--schema <arg>   json schema file
```
**schema**: JSON schema file  

**properties**: Kafka Streams properties file for bootstrap server address and all other [Kafka Streams Configuration Parameters](https://kafka.apache.org/10/documentation/streams/developer-guide/config-streams.html). if not provided looks for `stream.properties` by default.

**input**: Topic to validate against the schema.

**output**: Forwards validated messages to this topic.

**error**: If provided, puts invalid messages along with validation errors to this topic.

## Example

`schema.json` file:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema",
  "$id": "http://json-schema.org/draft-07/schema#",
  "title": "Kafka Json Schema Validator Stream - Test schema",
  "definitions": {
  },
  "type": "object",
  "properties": {
    "str": {
      "type": "string"
    },
    "number": {
      "type": "integer",
      "minimum": 0
    }
  },
  "required": [
    "str",
    "number"
  ]
}
```

`stream.properties` file:
```properties
security.protocol=PLAINTEXT
bootstrap.servers=localhost:9092
auto.offset.reset=earliest
application.id=JsonValidatorStream1
client.id=JsonValidatorStreamClient
default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
```
Run:
```sh
$ java -jar kafka-json-schema-validator-jar-with-dependencies.jar -s schema.json -i my-input -o my-output -e invalid
```
This command reads every message from `my-input` topic.

If the message is valid considering `schema.json` content, then it is forwarded to `my-output` topic as is.

If there are some validation errors, then puts the message along with the validation errors to `invalid` topic.

**Sample invalid JSON message sent to error topic**
```json
{
  "errors" : ["#: 2 schema violations found", "#/str: expected type: String, found: Integer", "#/number: -42 is not greater or equal to 0"],
  "inputRaw": "{\"str\": 123,\"number\": -42}"
}
```
