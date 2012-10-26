package com.jillesvangurp.geokv;

/**
 * API for serializing and parsing values to and from String. This is used in the GeoKV to be able to read and write values.
 * 
 * note. implementations should be thread safe and the serialized String MUST NOT contain new lines (i.e. escape them).
 * 
 * @param <Value> type of the value
 */
public interface ValueProcessor<Value> {
    String serialize(Value v);
    Value parse(String blob);
}
