package io.github.vovten.eventflow.util;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Lightweight JSON string builder that handles comma separation, quoting, and escaping.
 * <p>
 * Uses {@link StringBuilder} internally — no reflection, no Jackson.
 * Tracks comma state across nested objects and arrays automatically.
 */
public final class JsonBuilder {

    private final StringBuilder sb;
    private final Deque<Boolean> hasItemsStack;
    private boolean hasItems;

    public JsonBuilder() {
        this(64);
    }

    public JsonBuilder(int initialCapacity) {
        this.sb = new StringBuilder(initialCapacity);
        this.hasItemsStack = new ArrayDeque<>();
        this.hasItems = false;
    }

    /**
     * Begin a JSON object at the current level.
     *
     * @return this builder
     */
    public JsonBuilder beginObject() {
        sb.append('{');
        hasItemsStack.push(hasItems);
        hasItems = false;
        return this;
    }

    /**
     * Begin a named JSON object: {@code "key":{...}}.
     *
     * @param key the field name
     * @return this builder
     */
    public JsonBuilder beginObject(String key) {
        appendComma();
        appendQuoted(key);
        sb.append(":{");
        hasItemsStack.push(hasItems);
        hasItems = false;
        return this;
    }

    /**
     * Close the current JSON object.
     *
     * @return this builder
     */
    public JsonBuilder endObject() {
        sb.append('}');
        hasItems = hasItemsStack.pop();
        hasItems = true;
        return this;
    }

    /**
     * Begin a named JSON array: {@code "key":[...]}.
     *
     * @param key the field name
     * @return this builder
     */
    public JsonBuilder beginArray(String key) {
        appendComma();
        appendQuoted(key);
        sb.append(":[");
        hasItemsStack.push(hasItems);
        hasItems = false;
        return this;
    }

    /**
     * Close the current JSON array.
     *
     * @return this builder
     */
    public JsonBuilder endArray() {
        sb.append(']');
        hasItems = hasItemsStack.pop();
        hasItems = true;
        return this;
    }

    /**
     * Append a string field.
     * <p>
     * The value is automatically quoted and escaped.
     *
     * @param key   the field name
     * @param value the field value
     * @return this builder
     */
    public JsonBuilder appendString(String key, String value) {
        appendComma();
        appendQuoted(key);
        sb.append(':');
        appendQuoted(value);
        hasItems = true;
        return this;
    }

    /**
     * Append a numeric field.
     *
     * @param key   the field name
     * @param value the field value
     * @return this builder
     */
    public JsonBuilder appendNumber(String key, Number value) {
        appendComma();
        appendQuoted(key);
        sb.append(':');
        sb.append(value);
        hasItems = true;
        return this;
    }

    /**
     * Append a raw (pre-formatted) JSON value as a field.
     * <p>
     * The value is inserted as-is without quoting or escaping.
     *
     * @param key       the field name
     * @param jsonValue raw JSON value to insert
     * @return this builder
     */
    public JsonBuilder appendRaw(String key, String jsonValue) {
        appendComma();
        appendQuoted(key);
        sb.append(':');
        sb.append(jsonValue);
        hasItems = true;
        return this;
    }

    /**
     * Append a string item inside an array.
     * <p>
     * A comma is automatically added before the item if it is not the first in the array.
     *
     * @param value the array item value
     * @return this builder
     */
    public JsonBuilder appendArrayItem(String value) {
        appendComma();
        appendQuoted(value);
        hasItems = true;
        return this;
    }

    /**
     * Return the built JSON string.
     *
     * @return the JSON string
     */
    public String build() {
        return sb.toString();
    }

    @Override
    public String toString() {
        return sb.toString();
    }

    private void appendComma() {
        if (hasItems) {
            sb.append(',');
        }
    }

    private void appendQuoted(String value) {
        if (value == null) {
            sb.append("\"\"");
            return;
        }
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
    }
}
