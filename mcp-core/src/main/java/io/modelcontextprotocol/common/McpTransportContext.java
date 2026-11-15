/*
 * Copyright 2024-2025 the original author or authors.
 */

package io.modelcontextprotocol.common;

import java.util.Collections;
import java.util.Map;

import io.modelcontextprotocol.util.Assert;

/**
 * Context associated with the transport layer. It allows to add transport-level metadata
 * for use further down the line. Specifically, it can be beneficial to extract HTTP
 * request metadata for use in MCP feature implementations.
 * <p>
 * The context travels in the Reactor context, under {@link #KEY}. On the server side, the
 * transports populate it from the incoming request. On the client side, writing it is the
 * caller's responsibility:
 * <ul>
 * <li>with a synchronous client, configure
 * {@code McpClient.SyncSpec#transportContextProvider(Supplier)}, which is invoked on the
 * calling thread before every operation;
 * <li>with an asynchronous client, write it into the Reactor context once, where the
 * reactive chain starts, using
 * {@code contextWrite(ctx -> ctx.put(McpTransportContext.KEY, context))}. Every client
 * call downstream inherits it, so there is no need to repeat it at each call site.
 * </ul>
 *
 * @author Dariusz Jędrzejczyk
 */
public interface McpTransportContext {

	// 中文导读：保存本次传输关联的元数据，例如从 HTTP 请求提取的身份信息；它不是线上 JSON 的 _meta。
	// 异步路径通过 Reactor Context 按订阅传播，不能假设切换线程后 ThreadLocal 仍然可见。
	// 同步客户端可通过 transportContextProvider 在调用线程采集，再写入其内部异步调用链。

	/**
	 * Key for use in Reactor Context to transport the context to user land. Write the
	 * context under this key to make it visible to the transport, for example
	 * {@code contextWrite(ctx -> ctx.put(McpTransportContext.KEY, context))}.
	 */
	String KEY = "MCP_TRANSPORT_CONTEXT";

	/**
	 * An empty, unmodifiable context.
	 */
	@SuppressWarnings("unchecked")
	McpTransportContext EMPTY = new DefaultMcpTransportContext(Collections.EMPTY_MAP);

	/**
	 * Create an unmodifiable context containing the given metadata.
	 * @param metadata the transport metadata
	 * @return the context containing the metadata
	 */
	static McpTransportContext create(Map<String, Object> metadata) {
		return new DefaultMcpTransportContext(metadata);
	}

	/**
	 * Extract a value from the context.
	 * @param key the key under the data is expected
	 * @return the associated value or {@code null} if missing.
	 */
	Object get(String key);

	/**
	 * Create a new context holding the values of this context and of the specified map.
	 * When both hold the same key, the value from the provided map wins. Neither this
	 * context nor the provided map is modified. When the provided map is empty, this
	 * context is returned as is.
	 * <p>
	 * Keys absent from the provided map are looked up in this context. As with
	 * {@link #create(Map)}, {@code null} keys and values are rejected.
	 * @param map the values to add, taking precedence over the values of this context
	 * @return a new context holding the merged values
	 * @since 2.2.0
	 */
	default McpTransportContext putAll(Map<String, ?> map) {
		// 接口只提供 get，不能枚举任意实现的全部数据，因此默认用覆盖层保留对原 context 的查询能力。
		Assert.notNull(map, "The map cannot be null");
		if (map.isEmpty()) {
			return this;
		}
		return new OverlayMcpTransportContext(this, map);
	}

}
