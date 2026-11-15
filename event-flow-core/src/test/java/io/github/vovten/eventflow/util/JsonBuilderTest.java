package io.github.vovten.eventflow.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link JsonBuilder}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("JsonBuilder Tests")
class JsonBuilderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final char NUL = 0;
    private static final char BACKSPACE = 8;
    private static final char FORM_FEED = 12;
    private static final char UNIT_SEPARATOR = 0x1F;
    private static final char LINE_SEPARATOR = 0x2028;
    private static final char PARAGRAPH_SEPARATOR = 0x2029;

    @Test
    @DisplayName("DEFECT-9.1: Escapes every control character and U+2028/U+2029")
    void shouldEscapeAllControlCharacters() throws Exception {
        String value = "a" + NUL + "b" + BACKSPACE + "c" + FORM_FEED
                + "d" + UNIT_SEPARATOR + "e" + LINE_SEPARATOR + "f" + PARAGRAPH_SEPARATOR
                + "g\"h\\i\nj";

        String json = new JsonBuilder().beginObject().appendString("message", value).endObject().build();

        JsonNode parsed = MAPPER.readTree(json);
        assertThat(parsed.get("message").asText())
                .as("A log consumer must be able to parse the JSON without losing characters")
                .isEqualTo(value);
        assertThat(json)
                .doesNotContain(String.valueOf(NUL))
                .doesNotContain(String.valueOf(BACKSPACE))
                .doesNotContain(String.valueOf(FORM_FEED))
                .doesNotContain(String.valueOf(UNIT_SEPARATOR))
                .doesNotContain(String.valueOf(LINE_SEPARATOR))
                .doesNotContain(String.valueOf(PARAGRAPH_SEPARATOR));
    }

    @Test
    @DisplayName("DEFECT-9.1: Escapes control characters in array items as well")
    void shouldEscapeControlCharactersInArrayItems() throws Exception {
        String value = "x" + (char) 1 + "y" + LINE_SEPARATOR + "z";

        String json = new JsonBuilder().beginObject().beginArray("items").appendArrayItem(value)
                .endArray().endObject().build();

        JsonNode parsed = MAPPER.readTree(json);
        assertThat(parsed.get("items").get(0).asText()).isEqualTo(value);
        assertThat(json).doesNotContain(String.valueOf((char) 1)).doesNotContain(String.valueOf(LINE_SEPARATOR));
    }
}
