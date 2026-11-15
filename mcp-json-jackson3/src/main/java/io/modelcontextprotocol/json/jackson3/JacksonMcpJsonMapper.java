/*
 * Copyright 2026 - 2026 the original author or authors.
 */

package io.modelcontextprotocol.json.jackson3;

import java.io.IOException;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

/**
 * Jackson-based implementation of JsonMapper. Wraps a Jackson JsonMapper but keeps the
 * SDK decoupled from Jackson at the API level.
 */
public final class JacksonMcpJsonMapper implements McpJsonMapper {

	// 中文导读：将 SDK 的统一 JSON 接口适配到 Jackson 3（tools.jackson 包）。
	// 读写失败转成 IOException，对象转换失败转成 IllegalArgumentException；上层再映射协议错误。

	private final JsonMapper jsonMapper;

	/**
	 * Constructs a new JacksonMcpJsonMapper instance with the given JsonMapper.
	 * @param jsonMapper the JsonMapper to be used for JSON serialization and
	 * deserialization. Must not be null.
	 * @throws IllegalArgumentException if the provided JsonMapper is null.
	 */
	public JacksonMcpJsonMapper(JsonMapper jsonMapper) {
		if (jsonMapper == null) {
			throw new IllegalArgumentException("JsonMapper must not be null");
		}
		this.jsonMapper = jsonMapper;
	}

	/**
	 * Returns the underlying Jackson {@link JsonMapper} used for JSON serialization and
	 * deserialization.
	 * @return the JsonMapper instance
	 */
	public JsonMapper getJsonMapper() {
		return jsonMapper;
	}

	@Override
	public <T> T readValue(String content, Class<T> type) throws IOException {
		try {
			return jsonMapper.readValue(content, type);
		}
		catch (JacksonException ex) {
			throw new IOException("Failed to read value", ex);
		}
	}

	@Override
	public <T> T readValue(byte[] content, Class<T> type) throws IOException {
		try {
			return jsonMapper.readValue(content, type);
		}
		catch (JacksonException ex) {
			throw new IOException("Failed to read value", ex);
		}
	}

	@Override
	public <T> T readValue(String content, TypeRef<T> type) throws IOException {
		// 保留 TypeRef 中的泛型参数，否则反序列化集合时元素可能只得到 Map 而不是协议 record。
		JavaType javaType = jsonMapper.getTypeFactory().constructType(type.getType());
		try {
			return jsonMapper.readValue(content, javaType);
		}
		catch (JacksonException ex) {
			throw new IOException("Failed to read value", ex);
		}
	}

	@Override
	public <T> T readValue(byte[] content, TypeRef<T> type) throws IOException {
		JavaType javaType = jsonMapper.getTypeFactory().constructType(type.getType());
		try {
			return jsonMapper.readValue(content, javaType);
		}
		catch (JacksonException ex) {
			throw new IOException("Failed to read value", ex);
		}
	}

	@Override
	public <T> T convertValue(Object fromValue, Class<T> type) {
		try {
			return jsonMapper.convertValue(fromValue, type);
		}
		catch (JacksonException ex) {
			throw new IllegalArgumentException(ex);
		}
	}

	@Override
	public <T> T convertValue(Object fromValue, TypeRef<T> type) {
		JavaType javaType = jsonMapper.getTypeFactory().constructType(type.getType());
		try {
			return jsonMapper.convertValue(fromValue, javaType);
		}
		catch (JacksonException ex) {
			throw new IllegalArgumentException(ex);
		}
	}

	@Override
	public String writeValueAsString(Object value) throws IOException {
		try {
			return jsonMapper.writeValueAsString(value);
		}
		catch (JacksonException ex) {
			throw new IOException("Failed to write value as string", ex);
		}
	}

	@Override
	public byte[] writeValueAsBytes(Object value) throws IOException {
		try {
			return jsonMapper.writeValueAsBytes(value);
		}
		catch (JacksonException ex) {
			throw new IOException("Failed to write value as bytes", ex);
		}
	}

}
