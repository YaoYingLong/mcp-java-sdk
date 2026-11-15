/**
 * Copyright 2026 - 2026 the original author or authors.
 */
package io.modelcontextprotocol.json;

import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.json.schema.JsonSchemaValidatorSupplier;
import io.modelcontextprotocol.util.McpServiceLoader;

/**
 * This class is to be used to provide access to the default {@link McpJsonMapper} and to
 * the default {@link JsonSchemaValidator} instances via the static methods:
 * {@link #getMapper()} and {@link #getSchemaValidator()}.
 * <p>
 * The initialization of (singleton) instances of this class is different in non-OSGi
 * environments and OSGi environments. Specifically, in non-OSGi environments the
 * {@code McpJsonDefaults} class will be loaded by whatever classloader is used to call
 * one of the existing static get methods for the first time. For servers, this will
 * usually be in response to the creation of the first {@code McpServer} instance. At that
 * first time, the {@code mcpMapperServiceLoader} and {@code mcpValidatorServiceLoader}
 * will be null, and the {@code McpJsonDefaults} constructor will be called,
 * creating/initializing the {@code mcpMapperServiceLoader} and the
 * {@code mcpValidatorServiceLoader}...which will then be used to call the
 * {@code ServiceLoader.load} method.
 * <p>
 * In OSGi environments, upon bundle activation SCR will create a new (singleton) instance
 * of {@code McpJsonDefaults} (via the constructor), and then inject suppliers via the
 * {@code setMcpJsonMapperSupplier} and {@code setJsonSchemaValidatorSupplier} methods
 * with the SCR-discovered instances of those services. This does depend upon the
 * jars/bundles providing those suppliers to be started/activated. This SCR behavior is
 * dictated by xml files in {@code OSGi-INF} directory of {@code mcp-core} (this
 * project/jar/bundle), and the jsonmapper and jsonschemavalidator provider jars/bundles
 * (e.g. {@code mcp-json-jackson2}, {@code mcp-json-jackson3}, or others).
 */
public class McpJsonDefaults {

	// 中文导读：默认 JSON 实现的装配入口。普通 Java 环境走 ServiceLoader，OSGi 走 supplier 注入；
	// 实际选择和实例缓存见 McpServiceLoader。mcp-core 自身不包含 Jackson 数据绑定实现。

	protected static McpServiceLoader<McpJsonMapperSupplier, McpJsonMapper> mcpMapperServiceLoader;

	protected static McpServiceLoader<JsonSchemaValidatorSupplier, JsonSchemaValidator> mcpValidatorServiceLoader;

	public McpJsonDefaults() {
		mcpMapperServiceLoader = new McpServiceLoader<>(McpJsonMapperSupplier.class);
		mcpValidatorServiceLoader = new McpServiceLoader<>(JsonSchemaValidatorSupplier.class);
	}

	void setMcpJsonMapperSupplier(McpJsonMapperSupplier supplier) {
		mcpMapperServiceLoader.setSupplier(supplier);
	}

	void unsetMcpJsonMapperSupplier(McpJsonMapperSupplier supplier) {
		mcpMapperServiceLoader.unsetSupplier(supplier);
	}

	public synchronized static McpJsonMapper getMapper() {
		// 首次访问同时建立 mapper 和 validator 的加载器；具体实例各自在 getDefault 中延迟创建。
		if (mcpMapperServiceLoader == null) {
			new McpJsonDefaults();
		}
		return mcpMapperServiceLoader.getDefault();
	}

	void setJsonSchemaValidatorSupplier(JsonSchemaValidatorSupplier supplier) {
		mcpValidatorServiceLoader.setSupplier(supplier);
	}

	void unsetJsonSchemaValidatorSupplier(JsonSchemaValidatorSupplier supplier) {
		mcpValidatorServiceLoader.unsetSupplier(supplier);
	}

	public synchronized static JsonSchemaValidator getSchemaValidator() {
		if (mcpValidatorServiceLoader == null) {
			new McpJsonDefaults();
		}
		return mcpValidatorServiceLoader.getDefault();
	}

}
