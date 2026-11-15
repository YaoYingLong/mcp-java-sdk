/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.client;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.modelcontextprotocol.modern.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.modern.McpSchema.Implementation;
import io.modelcontextprotocol.modern.McpSchema.MetaKeys;
import io.modelcontextprotocol.util.Assert;
import org.jspecify.annotations.Nullable;

/**
 * Builds the {@code params._meta} block every modern request carries.
 */
// modern 不把版本/能力放在一次性握手中，而是在 params._meta 中逐请求声明。
// 应用自定义项先经过 applicationEntries；保留命名空间和 progressToken 由客户端独占。
// capabilities 来自当前调用配置的输入处理器，因此某次调用覆盖 handler 后，能力声明也随之改变。
final class RequestMeta {

	private static final String RESERVED_PREFIX = "io.modelcontextprotocol/";

	private RequestMeta() {
	}

	/**
	 * Copies the entries an application adds to the {@code _meta} of its requests.
	 * @throws IllegalArgumentException for {@code progressToken} and keys with the
	 * {@code io.modelcontextprotocol/} prefix, which the client sets itself
	 */
	static Map<String, Object> applicationEntries(Map<String, ?> entries) {
		Assert.notNull(entries, "meta must not be null");
		Map<String, Object> copy = new LinkedHashMap<>();
		entries.forEach((key, value) -> {
			Assert.hasText(key, "meta keys must not be empty");
			Assert.isTrue(!MetaKeys.PROGRESS_TOKEN.equals(key) && !key.startsWith(RESERVED_PREFIX),
					"meta key '" + key + "' is set by the client");
			copy.put(key, value);
		});
		return Collections.unmodifiableMap(copy);
	}

	/**
	 * The {@code _meta} of a request: the application's entries followed by the client's.
	 */
	static Map<String, Object> merged(Map<String, Object> applicationEntries, Map<String, Object> clientEntries) {
		if (applicationEntries.isEmpty()) {
			return clientEntries;
		}
		Map<String, Object> meta = new LinkedHashMap<>(applicationEntries);
		meta.putAll(clientEntries);
		return Collections.unmodifiableMap(meta);
	}

	/**
	 * The {@code _meta} of one request. Its values are the schema records, serialized
	 * with the rest of the request.
	 * @param progressToken a string or integer token, or {@code null} when nobody listens
	 * for progress
	 */
	static Map<String, Object> of(String protocolVersion, ClientCapabilities clientCapabilities,
			@Nullable Implementation clientInfo, @Nullable Object progressToken) {
		Assert.hasText(protocolVersion, "protocolVersion must not be empty");
		Assert.notNull(clientCapabilities, "clientCapabilities must not be null");
		Assert.isTrue(progressToken == null || progressToken instanceof String || progressToken instanceof Integer
				|| progressToken instanceof Long, "progressToken must be a string or an integer");
		// 这里放入 schema 对象而非预先编码的 JSON 字符串；最终随整个请求统一序列化。
		// 没有进度监听时不发送 token，服务器也就无需为这次调用产生进度通知。
		Map<String, Object> meta = new LinkedHashMap<>();
		meta.put(MetaKeys.PROTOCOL_VERSION, protocolVersion);
		meta.put(MetaKeys.CLIENT_CAPABILITIES, clientCapabilities);
		if (clientInfo != null) {
			meta.put(MetaKeys.CLIENT_INFO, clientInfo);
		}
		if (progressToken != null) {
			meta.put(MetaKeys.PROGRESS_TOKEN, progressToken);
		}
		return Collections.unmodifiableMap(meta);
	}

}
