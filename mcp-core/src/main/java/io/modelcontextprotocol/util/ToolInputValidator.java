/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.util;

import java.util.List;
import java.util.Map;

import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates tool input arguments against JSON schema.
 *
 * @author Andrei Shakirin
 */
public final class ToolInputValidator {

	private static final Logger logger = LoggerFactory.getLogger(ToolInputValidator.class);

	private ToolInputValidator() {
	}

	/**
	 * Validates tool arguments against the tool's input schema.
	 * @param tool the tool definition containing the input schema
	 * @param arguments the arguments to validate
	 * @param validateToolInputs whether validation is enabled
	 * @param validator the JSON schema validator (may be null)
	 * @return CallToolResult with isError=true if validation fails, null if valid or
	 * validation skipped
	 */
	public static CallToolResult validate(McpSchema.Tool tool, Map<String, Object> arguments,
			boolean validateToolInputs, JsonSchemaValidator validator) {
		// 中文导读：返回 null 表示允许继续调用工具（校验通过或被跳过），不是“工具返回空结果”。
		// 是否启用校验由服务端配置决定；arguments 缺省时以空对象匹配 inputSchema。
		if (!validateToolInputs || tool.inputSchema() == null || tool.inputSchema().isEmpty() || validator == null) {
			return null;
		}
		Map<String, Object> args = arguments != null ? arguments : Map.of();
		var validation = validator.validate(tool.inputSchema(), args);
		if (!validation.valid()) {
			// 参数不符合工具 schema 时返回工具级 isError 结果，仍通过 JSON-RPC result 发送。
			// 这与未知方法等 JSON-RPC error 不同，调用方不能只检查是否抛异常来判断工具成功。
			String message = "Tool (" + tool.name() + ") input validation failed: " + validation.errorMessage();
			logger.warn(message);
			return CallToolResult.builder()
				.content(List.of(McpSchema.TextContent.builder(message).build()))
				.isError(true)
				.build();
		}
		return null;
	}

}
