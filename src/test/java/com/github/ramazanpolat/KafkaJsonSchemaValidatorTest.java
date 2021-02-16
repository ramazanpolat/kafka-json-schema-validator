package com.github.ramazanpolat;

import org.everit.json.schema.Schema;
import org.junit.Test;

import java.util.ArrayList;


public class KafkaJsonSchemaValidatorTest {
    @Test
    public void testLoadSchema() {
        try {
            KafkaJsonSchemaValidator.loadSchema("testSchema1.json");
        } catch (Exception ex) {
            assert false;
        }
    }

    @Test
    public void testSchemaViolations1() {
        /* testing with a properly formatted and schema-valid JSON */
        String json = "{\"str\": \"A string\",\"number\": 42}";
        System.out.println(json);
        try {
            Schema schema = KafkaJsonSchemaValidator.loadSchema("testSchema1.json");
            ArrayList<String> violations = KafkaJsonSchemaValidator.validateString(schema, json);
            assert violations.size() == 0;
        } catch (Exception ex) {
            System.err.println("THIS MUST NOT TO BE PRINTED!");
            assert false;
        }
    }

    @Test
    public void testSchemaViolations2() {
        /* testing with an improperly formatted JSON */
        String jsonish = "{\"str: \"A string\",\"number\": 42}";
        try {
            Schema schema = KafkaJsonSchemaValidator.loadSchema("testSchema1.json");
            ArrayList<String> violations = KafkaJsonSchemaValidator.validateString(schema, jsonish);
            for (String s : violations)
                System.out.println("* " + s);
            assert violations.size() > 0;
        } catch (Exception ex) {
            System.err.println("THIS MUST NOT TO BE PRINTED!");
            assert false;
        }
    }

    @Test
    public void testSchemaViolations3() {
        /* testing with a properly formatted BUT schema-INVALID JSON */
        String json = "{\"str: \"A string\",\"number\": -42}";
        try {
            Schema schema = KafkaJsonSchemaValidator.loadSchema("testSchema1.json");
            ArrayList<String> violations = KafkaJsonSchemaValidator.validateString(schema, json);
            for (String s : violations)
                System.out.println("* " + s);

            assert violations.size() > 0;
        } catch (Exception ex) {
            System.err.println("THIS MUST NOT TO BE PRINTED!");
            assert false;
        }
    }

    @Test
    public void testSchemaViolations4() {
        /* testing with a properly formatted BUT schema-INVALID JSON with multiple violations*/
        String json = "{\"str\": 123,\"number\": -42}";
        try {
            Schema schema = KafkaJsonSchemaValidator.loadSchema("testSchema1.json");
            ArrayList<String> violations = KafkaJsonSchemaValidator.validateString(schema, json);
            for (String s : violations)
                System.out.println("* " + s);

            assert violations.size() > 0;
        } catch (Exception ex) {
            System.err.println("THIS MUST NOT TO BE PRINTED!");
            assert false;
        }
    }

}
