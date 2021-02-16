package com.github.ramazanpolat;

import org.apache.commons.cli.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONTokener;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class KafkaJsonSchemaValidator {
    public static Properties loadProps(String propsFile) {
        Properties prop = new Properties();
        try (InputStream inputStream = new FileInputStream(propsFile)) {
            prop.load(inputStream);
        } catch (IOException ex) {
            System.err.println("Cannot open properties file: " + propsFile);
            ex.printStackTrace();
            System.exit(43);
        }
        return prop;
    }

    public static Schema loadSchema(String schemaFile) {
        Schema schema = null;
        try (InputStream inputStream = KafkaJsonSchemaValidator.class.getClassLoader().getResourceAsStream(schemaFile)) {
            assert inputStream != null;
            JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
            SchemaLoader loader = SchemaLoader.builder()
                    .schemaJson(rawSchema)
                    .draftV6Support() // or draftV7Support()
                    .build();
            schema = loader.load().build();
        } catch (Exception e) {
            System.err.println("Cannot load schema file: " + schemaFile);
            System.exit(42);
        }
        return schema;
    }

    public static ArrayList<String> validateString(Schema schema1, String rawInput) {
        System.out.println("Validating: " + rawInput);
        ArrayList<String> violations = new ArrayList<>();
        try {
            schema1.validate(new JSONObject(rawInput));
            //System.out.println("Valid: " + rawInput);
        } catch (ValidationException e) {
            System.err.println("Error on validateSchema1: " + e.getMessage());
            violations.add(e.getMessage());
            e.getCausingExceptions().stream()
                    .map(ValidationException::getMessage)
                    .forEach(violations::add);
        } catch (Exception e) {
            System.err.println("Error on validateSchema2: " + e.getMessage());
            violations.add("This is not a properly formatted JSON: " + e.getMessage());
        }
        if (violations.size() > 0)
            violations.forEach(System.err::println);
        return violations;
    }

    public static String violationsJSON(List<String> violations, String inputRaw) {
        String result = "{\"errors\":[\"Could not generate violationsJSON\"], \"inputRaw\": \"\"}";
        try {
            JSONObject jo = new JSONObject();
            jo.put("errors", violations);
            jo.put("inputRaw", inputRaw);
            result = jo.toString();
        } catch (Exception e) {
            System.err.println("Exception in violationsJSON: " + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    public static void startStream(Properties props, Schema schema, String inputTopic, String outputTopic, String errorTopic) {
        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        // Stream from inputTopic
        KStream<String, String> rawInput = builder.stream(inputTopic);

        // if errorTopic is provided
        if (errorTopic != null && errorTopic.length() > 0) {
            // Branch on JSON validation.
            @SuppressWarnings("unchecked")
            KStream<String, String>[] validOrNot = rawInput.branch(
                    (key, value) -> validateString(schema, value).size() == 0, // valid
                    (key, value) -> validateString(schema, value).size() > 0 // invalid
            );

            validOrNot[0].to(outputTopic);
            validOrNot[1].mapValues(value -> violationsJSON(validateString(schema, value), value)).to(errorTopic);
        } else {
            // if errorTopic is not provided, then just filter valid messages
            rawInput.filter((key, value) -> validateString(schema, value).size() == 0).to(outputTopic);
        }

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing the stream...");
            streams.close();
            System.out.println("Stream closed.");
        }));

        System.out.println("Input topic:" + inputTopic);
        System.out.println("Output topic:" + outputTopic);
        System.out.println("Invalid topic:" + errorTopic);
        // Now run the processing topology via `start()` to begin processing its input data.
        streams.start();
        System.out.println("Validating...");
    }

    public static void main(final String[] args) {
        /* DEFINE CLI PARAMS */
        Options options = new Options();

        Option schemaArg = new Option("s", "schema", true, "json schema file");
        schemaArg.setRequired(true);
        options.addOption(schemaArg);

        Option inputTopicArg = new Option("i", "input", true, "input topic");
        inputTopicArg.setRequired(true);
        options.addOption(inputTopicArg);

        Option outputTopicArg = new Option("o", "output", true, "output topic");
        outputTopicArg.setRequired(true);
        options.addOption(outputTopicArg);

        Option invalidTopicArg = new Option("e", "error", true, "error(invalid) topic [not required]");
        invalidTopicArg.setRequired(false);
        options.addOption(invalidTopicArg);

        Option propertiesArg = new Option("p", "properties", false, "properties (default='stream.properties')");
        propertiesArg.setRequired(false);
        options.addOption(propertiesArg);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        /* PARSE CLI PARAMS */
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java -jar JsonValidatorStream.jar", options);
            System.exit(1);
        }

        /* GET CLI PARAMS */
        String schemaFile = cmd.getOptionValue("schema");
        String inputTopic = cmd.getOptionValue("input");
        String outputTopic = cmd.getOptionValue("output");
        String errorTopic = cmd.getOptionValue("error");
        String propertiesFile = cmd.getOptionValue("properties");
        if (propertiesFile == null)
            propertiesFile = "stream.properties";

        /* PRINT CLI PARAMS */
        System.out.println("JSON schema: " + inputTopic);
        System.out.println("input topic: " + inputTopic);
        System.out.println("output topic: " + outputTopic);
        if (errorTopic != null)
            System.out.println("error topic: " + errorTopic);
        else
            System.out.println("error topic: [skip invalidated messages]");

        Schema schemaObj = loadSchema(schemaFile);
        startStream(loadProps(propertiesFile), schemaObj, inputTopic, outputTopic, errorTopic);
    }

/*    public static void saveProps(String filename) {
        Properties properties = getStreamsConfiguration();
        try (OutputStream outputStream = new FileOutputStream("stream.properties")) {
            properties.store(outputStream, "Default stream properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
        /*
    static Properties getStreamsConfiguration() {
        final String bootstrapServers = "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "JsonValidatorStream1");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "JsonValidatorStreamClient");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(10 * 1000));
        // For illustrative purposes we disable record caches.
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        // Use a temporary directory for storing state, which will be automatically removed after the test.
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }*/
}
