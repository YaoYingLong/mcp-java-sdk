/*
 * Copyright 2024-2026 the original author or authors.
 */

package io.modelcontextprotocol.server;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.spec.DefaultMcpStreamableServerSessionFactory;
import io.modelcontextprotocol.spec.McpClientSession;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.CompleteResult.CompleteCompletion;
import io.modelcontextprotocol.spec.McpSchema.ErrorCodes;
import io.modelcontextprotocol.spec.McpSchema.PromptReference;
import io.modelcontextprotocol.spec.McpSchema.ResourceReference;
import io.modelcontextprotocol.spec.McpSchema.SetLevelRequest;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.McpServerSession;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import io.modelcontextprotocol.spec.McpServerTransportProviderBase;
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider;
import io.modelcontextprotocol.util.Assert;
import io.modelcontextprotocol.util.DefaultMcpUriTemplateManagerFactory;
import io.modelcontextprotocol.util.McpUriTemplateManagerFactory;
import io.modelcontextprotocol.util.ToolInputValidator;
import io.modelcontextprotocol.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.modelcontextprotocol.spec.McpError.RESOURCE_NOT_FOUND;

/**
 * The Model Context Protocol (MCP) server implementation that provides asynchronous
 * communication using Project Reactor's Mono and Flux types.
 *
 * <p>
 * This server implements the MCP specification, enabling AI models to expose tools,
 * resources, and prompts through a standardized interface. Key features include:
 * <ul>
 * <li>Asynchronous communication using reactive programming patterns
 * <li>Dynamic tool registration and management
 * <li>Resource handling with URI-based addressing
 * <li>Prompt template management
 * <li>Real-time client notifications for state changes
 * <li>Structured logging with configurable severity levels
 * <li>Support for client-side AI model sampling
 * </ul>
 *
 * <p>
 * The server follows a lifecycle:
 * <ol>
 * <li>Initialization - Accepts client connections and negotiates capabilities
 * <li>Normal Operation - Handles client requests and sends notifications
 * <li>Graceful Shutdown - Ensures clean connection termination
 * </ol>
 *
 * <p>
 * This implementation uses Project Reactor for non-blocking operations, making it
 * suitable for high-throughput scenarios and reactive applications. All operations return
 * Mono or Flux types that can be composed into reactive pipelines.
 *
 * <p>
 * The server supports runtime modification of its capabilities through methods like
 * {@link #addTool}, {@link #addResource}, and {@link #addPrompt}, automatically notifying
 * connected clients of changes when configured to do so.
 *
 * @author Christian Tzolov
 * @author Dariusz Jędrzejczyk
 * @author Jihoon Kim
 * @see McpServer
 * @see McpSchema
 * @see McpClientSession
 */
/*
 * 服务端业务路由中心：保存全服务器共享的工具/资源/提示词，创建 method -> handler 表， 并把 Session 工厂交给 Provider。Provider
 * 管理连接及会话集合，Session 管理 JSON-RPC 关联， Exchange 则把本次调用可用的客户端能力、传输上下文和反向调用入口交给业务函数。 调试
 * tools/call 可依次跟踪：传输解包 -> Session 路由 -> toolsCallRequestHandler -> 注册的 handler。
 */
public class McpAsyncServer {

	private static final Logger logger = LoggerFactory.getLogger(McpAsyncServer.class);

	private final McpServerTransportProviderBase mcpTransportProvider;

	private final McpJsonMapper jsonMapper;

	private final JsonSchemaValidator jsonSchemaValidator;

	private final boolean validateToolInputs;

	private final McpSchema.ServerCapabilities serverCapabilities;

	private final McpSchema.Implementation serverInfo;

	private final String instructions;

	private final CopyOnWriteArrayList<McpServerFeatures.AsyncToolSpecification> tools = new CopyOnWriteArrayList<>();

	private final ConcurrentHashMap<String, McpServerFeatures.AsyncResourceSpecification> resources = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<String, McpServerFeatures.AsyncResourceTemplateSpecification> resourceTemplates = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<String, McpServerFeatures.AsyncPromptSpecification> prompts = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<McpSchema.CompleteReference, McpServerFeatures.AsyncCompletionSpecification> completions = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<String, Set<String>> resourceSubscriptions = new ConcurrentHashMap<>();

	private final McpAsyncListFilter<McpSchema.Tool> toolFilter;

	private List<String> protocolVersions;

	private McpUriTemplateManagerFactory uriTemplateManagerFactory = new DefaultMcpUriTemplateManagerFactory();

	/**
	 * Create a new McpAsyncServer with the given transport provider and capabilities.
	 * @param mcpTransportProvider The transport layer implementation for MCP
	 * communication.
	 * @param features The MCP server supported features.
	 * @param jsonMapper The JsonMapper to use for JSON serialization/deserialization
	 */
	@SuppressWarnings("deprecation")
	McpAsyncServer(McpServerTransportProvider mcpTransportProvider, McpJsonMapper jsonMapper,
			McpServerFeatures.Async features, Duration requestTimeout,
			McpUriTemplateManagerFactory uriTemplateManagerFactory, JsonSchemaValidator jsonSchemaValidator,
			boolean validateToolInputs) {
		this.mcpTransportProvider = mcpTransportProvider;
		this.jsonMapper = jsonMapper;
		this.serverInfo = features.serverInfo();
		// Logging is deprecated by MCP 2026-07-28 (SEP-2577) but is part of every
		// protocol
		// version this server negotiates, so the capability is still always advertised.
		this.serverCapabilities = features.serverCapabilities().mutate().logging().build();
		this.instructions = features.instructions();
		this.tools.addAll(withStructuredOutputHandling(jsonSchemaValidator, features.tools()));
		this.resources.putAll(features.resources());
		this.resourceTemplates.putAll(features.resourceTemplates());
		this.prompts.putAll(features.prompts());
		this.completions.putAll(features.completions());
		this.uriTemplateManagerFactory = uriTemplateManagerFactory;
		this.jsonSchemaValidator = jsonSchemaValidator;
		this.validateToolInputs = validateToolInputs;
		this.toolFilter = McpAsyncListFilter.and(features.toolFilters());

		Map<String, McpRequestHandler<?>> requestHandlers = prepareRequestHandlers();
		Map<String, McpNotificationHandler> notificationHandlers = prepareNotificationHandlers(features);

		this.protocolVersions = mcpTransportProvider.protocolVersions();

		// Provider 确定一个客户端连接后再调用工厂；路由表由各会话共享，握手状态与请求关联表归各自会话。
		// stdio 在 setSessionFactory 中立即创建唯一会话并启动输入读取；旧 SSE 在 GET 建连时创建会话。
		mcpTransportProvider.setSessionFactory(transport -> {
			String sessionId = UUID.randomUUID().toString();
			return new McpServerSession(sessionId, requestTimeout, transport, this::asyncInitializeRequestHandler,
					requestHandlers, notificationHandlers, () -> this.cleanupForSession(sessionId),
					this.jsonSchemaValidator);
		});
	}

	@SuppressWarnings("deprecation")
	McpAsyncServer(McpStreamableServerTransportProvider mcpTransportProvider, McpJsonMapper jsonMapper,
			McpServerFeatures.Async features, Duration requestTimeout,
			McpUriTemplateManagerFactory uriTemplateManagerFactory, JsonSchemaValidator jsonSchemaValidator,
			boolean validateToolInputs) {
		this.mcpTransportProvider = mcpTransportProvider;
		this.jsonMapper = jsonMapper;
		this.serverInfo = features.serverInfo();
		// Logging is deprecated by MCP 2026-07-28 (SEP-2577) but is part of every
		// protocol
		// version this server negotiates, so the capability is still always advertised.
		this.serverCapabilities = features.serverCapabilities().mutate().logging().build();
		this.instructions = features.instructions();
		this.tools.addAll(withStructuredOutputHandling(jsonSchemaValidator, features.tools()));
		this.resources.putAll(features.resources());
		this.resourceTemplates.putAll(features.resourceTemplates());
		this.prompts.putAll(features.prompts());
		this.completions.putAll(features.completions());
		this.uriTemplateManagerFactory = uriTemplateManagerFactory;
		this.jsonSchemaValidator = jsonSchemaValidator;
		this.validateToolInputs = validateToolInputs;
		this.toolFilter = McpAsyncListFilter.and(features.toolFilters());

		Map<String, McpRequestHandler<?>> requestHandlers = prepareRequestHandlers();
		Map<String, McpNotificationHandler> notificationHandlers = prepareNotificationHandlers(features);

		this.protocolVersions = mcpTransportProvider.protocolVersions();

		// Streamable HTTP 的初始化 POST 尚未绑定长期 SSE 连接，因此工厂返回会话及初始化结果 Mono，
		// 后续 GET 与普通 POST 再分别给同一会话挂接监听流或请求响应流。
		mcpTransportProvider.setSessionFactory(new DefaultMcpStreamableServerSessionFactory(requestTimeout,
				this::asyncInitializeRequestHandler, requestHandlers, notificationHandlers,
				sessionId -> this.cleanupForSession(sessionId), this.jsonSchemaValidator));
	}

	private Map<String, McpNotificationHandler> prepareNotificationHandlers(McpServerFeatures.Async features) {
		Map<String, McpNotificationHandler> notificationHandlers = new HashMap<>();

		notificationHandlers.put(McpSchema.METHOD_NOTIFICATION_INITIALIZED, (exchange, params) -> Mono.empty());

		List<BiFunction<McpAsyncServerExchange, List<McpSchema.Root>, Mono<Void>>> rootsChangeConsumers = features
			.rootsChangeConsumers();

		if (Utils.isEmpty(rootsChangeConsumers)) {
			rootsChangeConsumers = List.of((exchange, roots) -> Mono.fromRunnable(() -> logger
				.warn("Roots list changed notification, but no consumers provided. Roots list changed: {}", roots)));
		}

		notificationHandlers.put(McpSchema.METHOD_NOTIFICATION_ROOTS_LIST_CHANGED,
				asyncRootsListChangedNotificationHandler(rootsChangeConsumers));
		return notificationHandlers;
	}

	// 能力声明同时决定哪些协议方法被注册；缺少能力时不是返回空列表，而是根本没有对应路由。
	// addTool 等动态注册改变功能集合，但不能凭空启用构造时未声明的能力。
	private Map<String, McpRequestHandler<?>> prepareRequestHandlers() {
		Map<String, McpRequestHandler<?>> requestHandlers = new HashMap<>();

		// Initialize request handlers for standard MCP methods

		// Ping MUST respond with an empty data, but not NULL response.
		requestHandlers.put(McpSchema.METHOD_PING, (exchange, params) -> Mono.just(Map.of()));

		// Add tools API handlers if the tool capability is enabled
		if (this.serverCapabilities.tools() != null) {
			requestHandlers.put(McpSchema.METHOD_TOOLS_LIST, toolsListRequestHandler());
			requestHandlers.put(McpSchema.METHOD_TOOLS_CALL, toolsCallRequestHandler());
		}

		// Add resources API handlers if provided
		if (this.serverCapabilities.resources() != null) {
			requestHandlers.put(McpSchema.METHOD_RESOURCES_LIST, resourcesListRequestHandler());
			requestHandlers.put(McpSchema.METHOD_RESOURCES_READ, resourcesReadRequestHandler());
			requestHandlers.put(McpSchema.METHOD_RESOURCES_TEMPLATES_LIST, resourceTemplateListRequestHandler());
			if (Boolean.TRUE.equals(this.serverCapabilities.resources().subscribe())) {
				requestHandlers.put(McpSchema.METHOD_RESOURCES_SUBSCRIBE, resourcesSubscribeRequestHandler());
				requestHandlers.put(McpSchema.METHOD_RESOURCES_UNSUBSCRIBE, resourcesUnsubscribeRequestHandler());
			}
		}

		// Add prompts API handlers if provider exists
		if (this.serverCapabilities.prompts() != null) {
			requestHandlers.put(McpSchema.METHOD_PROMPT_LIST, promptsListRequestHandler());
			requestHandlers.put(McpSchema.METHOD_PROMPT_GET, promptsGetRequestHandler());
		}

		// Add logging API handlers if the logging capability is enabled
		if (this.serverCapabilities.logging() != null) {
			requestHandlers.put(McpSchema.METHOD_LOGGING_SET_LEVEL, setLoggerRequestHandler());
		}

		// Add completion API handlers if the completion capability is enabled
		if (this.serverCapabilities.completions() != null) {
			requestHandlers.put(McpSchema.METHOD_COMPLETION_COMPLETE, completionCompleteRequestHandler());
		}
		return requestHandlers;
	}

	// ---------------------------------------
	// Lifecycle Management
	// ---------------------------------------
	// 只生成服务器一侧的协商结果。客户端能力的保存、initialized 通知及 Exchange 建立由 Session 负责。
	// 支持版本来自具体传输；请求版本不支持时返回列表最后一个版本，客户端再决定能否继续。
	private Mono<McpSchema.InitializeResult> asyncInitializeRequestHandler(
			McpSchema.InitializeRequest initializeRequest) {
		return Mono.defer(() -> {
			logger.info("Client initialize request - Protocol: {}, Capabilities: {}, Info: {}",
					initializeRequest.protocolVersion(), initializeRequest.capabilities(),
					initializeRequest.clientInfo());

			// The server MUST respond with the highest protocol version it supports
			// if
			// it does not support the requested (e.g. Client) version.
			String serverProtocolVersion = this.protocolVersions.get(this.protocolVersions.size() - 1);

			if (this.protocolVersions.contains(initializeRequest.protocolVersion())) {
				// If the server supports the requested protocol version, it MUST
				// respond
				// with the same version.
				serverProtocolVersion = initializeRequest.protocolVersion();
			}
			else {
				logger.warn(
						"Client requested unsupported protocol version: {}, so the server will suggest the {} version instead",
						initializeRequest.protocolVersion(), serverProtocolVersion);
			}

			return Mono.just(new McpSchema.InitializeResult(serverProtocolVersion, this.serverCapabilities,
					this.serverInfo, this.instructions));
		});
	}

	/**
	 * Get the server capabilities that define the supported features and functionality.
	 * @return The server capabilities
	 */
	public McpSchema.ServerCapabilities getServerCapabilities() {
		return this.serverCapabilities;
	}

	/**
	 * Get the server implementation information.
	 * @return The server implementation details
	 */
	public McpSchema.Implementation getServerInfo() {
		return this.serverInfo;
	}

	/**
	 * Gracefully closes the server, allowing any in-progress operations to complete.
	 * @return A Mono that completes when the server has been closed
	 */
	// 关闭入口交给拥有会话及传输资源的 Provider；完整关闭流程需订阅返回的 Mono。
	// 是否在方法调用时先设置关闭标志取决于 Provider，例如旧 SSE 实现会立即设置该标志。
	public Mono<Void> closeGracefully() {
		return this.mcpTransportProvider.closeGracefully();
	}

	/**
	 * Close the server immediately.
	 */
	public void close() {
		this.mcpTransportProvider.close();
	}

	// Roots are deprecated by MCP 2026-07-28 (SEP-2577) but still supported for clients
	// that declare them.
	@SuppressWarnings("deprecation")
	private McpNotificationHandler asyncRootsListChangedNotificationHandler(
			List<BiFunction<McpAsyncServerExchange, List<McpSchema.Root>, Mono<Void>>> rootsChangeConsumers) {
		return (exchange, params) -> exchange.listRoots()
			.flatMap(listRootsResult -> Flux.fromIterable(rootsChangeConsumers)
				.flatMap(consumer -> Mono.defer(() -> consumer.apply(exchange, listRootsResult.roots())))
				.onErrorResume(error -> {
					logger.error("Error handling roots list change notification", error);
					return Mono.empty();
				})
				.then());
	}

	// ---------------------------------------
	// Tool Management
	// ---------------------------------------

	/**
	 * Add a new tool call specification at runtime.
	 * @param toolSpecification The tool specification to add
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> addTool(McpServerFeatures.AsyncToolSpecification toolSpecification) {
		if (toolSpecification == null) {
			return Mono.error(new IllegalArgumentException("Tool specification must not be null"));
		}
		if (toolSpecification.tool() == null) {
			return Mono.error(new IllegalArgumentException("Tool must not be null"));
		}
		if (toolSpecification.callHandler() == null) {
			return Mono.error(new IllegalArgumentException("Tool call handler must not be null"));
		}
		if (this.serverCapabilities.tools() == null) {
			return Mono.error(new IllegalStateException("Server must be configured with tool capabilities"));
		}

		try {
			var t = toolSpecification.tool();
			this.jsonSchemaValidator.assertConforms("Tool '" + t.name() + "' inputSchema", t.inputSchema());
			this.jsonSchemaValidator.assertConforms("Tool '" + t.name() + "' outputSchema", t.outputSchema());
		}
		catch (IllegalArgumentException e) {
			return Mono.error(e);
		}

		// 动态添加与初始注册走同一输出包装器；真正替换工具及发送 list_changed 在下方 defer 订阅时发生。
		// listChanged 只控制变更通知，工具集合本身仍会更新。
		var wrappedToolSpecification = withStructuredOutputHandling(this.jsonSchemaValidator, toolSpecification);

		return Mono.defer(() -> {
			// Remove tools with duplicate tool names first
			if (this.tools.removeIf(th -> th.tool().name().equals(wrappedToolSpecification.tool().name()))) {
				logger.warn("Replace existing Tool with name '{}'", wrappedToolSpecification.tool().name());
			}

			this.tools.add(wrappedToolSpecification);
			logger.debug("Added tool handler: {}", wrappedToolSpecification.tool().name());

			if (Boolean.TRUE.equals(this.serverCapabilities.tools().listChanged())) {
				return notifyToolsListChanged();
			}
			return Mono.empty();
		});
	}

	// 统一处理工具输出契约：业务已返回 isError=true 时直接透传；声明 outputSchema 时，
	// structuredContent 缺失或不符合 Schema 转为工具执行错误结果，而不是抛 JSON-RPC 错误。
	// 有结构化结果却没有 content 时补等价文本，便于只理解旧内容块的客户端消费。
	private static class StructuredOutputCallToolHandler
			implements BiFunction<McpAsyncServerExchange, McpSchema.CallToolRequest, Mono<McpSchema.CallToolResult>> {

		private final BiFunction<McpAsyncServerExchange, McpSchema.CallToolRequest, Mono<McpSchema.CallToolResult>> delegateCallToolResult;

		private final JsonSchemaValidator jsonSchemaValidator;

		private final Map<String, Object> outputSchema;

		public StructuredOutputCallToolHandler(JsonSchemaValidator jsonSchemaValidator,
				Map<String, Object> outputSchema,
				BiFunction<McpAsyncServerExchange, McpSchema.CallToolRequest, Mono<McpSchema.CallToolResult>> delegateHandler) {

			Assert.notNull(jsonSchemaValidator, "JsonSchemaValidator must not be null");
			Assert.notNull(delegateHandler, "Delegate call tool result handler must not be null");

			this.delegateCallToolResult = delegateHandler;
			this.outputSchema = outputSchema;
			this.jsonSchemaValidator = jsonSchemaValidator;
		}

		@Override
		public Mono<CallToolResult> apply(McpAsyncServerExchange exchange, McpSchema.CallToolRequest request) {

			return this.delegateCallToolResult.apply(exchange, request).map(result -> {

				if (Boolean.TRUE.equals(result.isError())) {
					// If the tool call resulted in an error, skip further validation
					return result;
				}

				if (outputSchema == null) {
					if (result.structuredContent() != null) {
						logger.warn(
								"Tool call with no outputSchema is not expected to have a result with structured content, but got: {}",
								result.structuredContent());
					}
					// Pass through. No validation is required if no output schema is
					// provided.
					return result;
				}

				// If an output schema is provided, servers MUST provide structured
				// results that conform to this schema.
				// https://modelcontextprotocol.io/specification/2025-06-18/server/tools#output-schema
				if (result.structuredContent() == null) {
					String content = "Response missing structured content which is expected when calling tool with non-empty outputSchema";
					logger.warn(content);
					return CallToolResult.builder()
						.content(List.of(McpSchema.TextContent.builder(content).build()))
						.isError(true)
						.build();
				}

				// Validate the result against the output schema
				var validation = this.jsonSchemaValidator.validate(outputSchema, result.structuredContent());

				if (!validation.valid()) {
					String message = "Tool (" + request.name() + ") output validation failed: "
							+ validation.errorMessage();
					logger.warn(message);
					return CallToolResult.builder()
						.content(List.of(McpSchema.TextContent.builder(message).build()))
						.isError(true)
						.build();
				}

				if (Utils.isEmpty(result.content())) {
					// For backwards compatibility, a tool that returns structured
					// content SHOULD also return functionally equivalent unstructured
					// content. (For example, serialized JSON can be returned in a
					// TextContent block.)
					// https://modelcontextprotocol.io/specification/2025-06-18/server/tools#structured-content

					return CallToolResult.builder()
						.content(List.of(McpSchema.TextContent.builder(validation.jsonStructuredOutput()).build()))
						.isError(result.isError())
						.structuredContent(result.structuredContent())
						.build();
				}

				return result;
			});
		}

	}

	private static List<McpServerFeatures.AsyncToolSpecification> withStructuredOutputHandling(
			JsonSchemaValidator jsonSchemaValidator, List<McpServerFeatures.AsyncToolSpecification> tools) {

		if (Utils.isEmpty(tools)) {
			return tools;
		}

		return tools.stream().map(tool -> withStructuredOutputHandling(jsonSchemaValidator, tool)).toList();
	}

	private static McpServerFeatures.AsyncToolSpecification withStructuredOutputHandling(
			JsonSchemaValidator jsonSchemaValidator, McpServerFeatures.AsyncToolSpecification toolSpecification) {

		if (toolSpecification.callHandler() instanceof StructuredOutputCallToolHandler) {
			// If the tool is already wrapped, return it as is
			return toolSpecification;
		}

		if (toolSpecification.tool().outputSchema() == null) {
			// If the tool does not have an output schema, return it as is
			return toolSpecification;
		}

		return McpServerFeatures.AsyncToolSpecification.builder()
			.tool(toolSpecification.tool())
			.callHandler(new StructuredOutputCallToolHandler(jsonSchemaValidator,
					toolSpecification.tool().outputSchema(), toolSpecification.callHandler()))
			.build();
	}

	/**
	 * List all registered tools.
	 * @return A Flux stream of all registered tools
	 */
	public Flux<Tool> listTools() {
		return Flux.fromIterable(this.tools).map(McpServerFeatures.AsyncToolSpecification::tool);
	}

	/**
	 * Remove a tool handler at runtime.
	 * @param toolName The name of the tool handler to remove
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> removeTool(String toolName) {
		if (toolName == null) {
			return Mono.error(new IllegalArgumentException("Tool name must not be null"));
		}
		if (this.serverCapabilities.tools() == null) {
			return Mono.error(new IllegalStateException("Server must be configured with tool capabilities"));
		}

		return Mono.defer(() -> {
			if (this.tools.removeIf(toolSpecification -> toolSpecification.tool().name().equals(toolName))) {
				logger.debug("Removed tool handler: {}", toolName);
				if (Boolean.TRUE.equals(this.serverCapabilities.tools().listChanged())) {
					return notifyToolsListChanged();
				}
			}
			else {
				logger.warn("Failed to remove tool with name '{}' (not found)", toolName);
			}

			return Mono.empty();
		});
	}

	/**
	 * Notifies clients that the list of available tools has changed.
	 * @return A Mono that completes when all clients have been notified
	 */
	public Mono<Void> notifyToolsListChanged() {
		return this.mcpTransportProvider.notifyClients(McpSchema.METHOD_NOTIFICATION_TOOLS_LIST_CHANGED, null);
	}

	// tools/list 的过滤使用本次请求上下文；仅控制列表展示，不替代 tools/call handler 内的访问控制。
	// 当前返回全部可见工具，尚未实现游标分页。
	private McpRequestHandler<McpSchema.ListToolsResult> toolsListRequestHandler() {
		return (exchange, params) -> {
			// TODO: Implement pagination. Cursors must be computed over the filtered
			// view, otherwise page offsets leak the number of hidden tools.
			return Flux.fromIterable(this.tools)
				.map(McpServerFeatures.AsyncToolSpecification::tool)
				.filterWhen(tool -> this.toolFilter.isVisible(exchange.transportContext(), tool)
					.onErrorResume(error -> opaqueListFilterError(tool, error)))
				.collectList()
				.map(tools -> McpSchema.ListToolsResult.builder(tools).build());
		};
	}

	/**
	 * Report a list filter failure to the client as an opaque {@code -32603} error, so
	 * that filter internals such as identity provider hostnames or the reason a principal
	 * was rejected never leave the server. The actual cause is logged instead. An
	 * {@link McpError} is deliberate on the filter's part and passes through untouched.
	 */
	private static Mono<Boolean> opaqueListFilterError(Tool tool, Throwable error) {
		if (error instanceof McpError mcpError && mcpError.getJsonRpcError() != null) {
			logger.debug("Tool list filter failed for tool '{}' with an explicit MCP error", tool.name(), error);
			return Mono.error(mcpError);
		}
		logger.error("Tool list filter failed for tool '{}', failing the tools/list request", tool.name(), error);
		return Mono.error(McpError.builder(ErrorCodes.INTERNAL_ERROR).message("Internal error").build());
	}

	// params 先转为完整 CallToolRequest，业务函数收到的是请求对象（包含 arguments 等），不是裸参数 Map。
	// 错误分层：未知工具 -> INVALID_PARAMS 的 JSON-RPC error；输入不符合工具 Schema ->
	// 正常 JSON-RPC result 内的 CallToolResult(isError=true)；handler 异常再由 Session 转为协议错误。
	private McpRequestHandler<CallToolResult> toolsCallRequestHandler() {
		return (exchange, params) -> {
			McpSchema.CallToolRequest callToolRequest = jsonMapper.convertValue(params,
					new TypeRef<McpSchema.CallToolRequest>() {
					});

			Optional<McpServerFeatures.AsyncToolSpecification> toolSpecification = this.tools.stream()
				.filter(tr -> callToolRequest.name().equals(tr.tool().name()))
				.findAny();

			if (toolSpecification.isEmpty()) {
				return Mono.error(McpError.builder(McpSchema.ErrorCodes.INVALID_PARAMS)
					.message("Unknown tool: invalid_tool_name")
					.data("Tool not found: " + callToolRequest.name())
					.build());
			}

			McpSchema.Tool tool = toolSpecification.get().tool();
			// Validator 校验时把 null arguments 视为空 Map，但不会改写原请求；通过后 handler 仍收到原对象。
			// validateToolInputs=false 会跳过此校验；失败时直接返回，业务 handler 不会执行。
			CallToolResult validationError = ToolInputValidator.validate(tool, callToolRequest.arguments(),
					this.validateToolInputs, this.jsonSchemaValidator);
			if (validationError != null) {
				return Mono.just(validationError);
			}

			return toolSpecification.get().callHandler().apply(exchange, callToolRequest);
		};
	}

	// ---------------------------------------
	// Resource Management
	// ---------------------------------------

	/**
	 * Add a new resource handler at runtime.
	 * @param resourceSpecification The resource handler to add
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> addResource(McpServerFeatures.AsyncResourceSpecification resourceSpecification) {
		if (resourceSpecification == null || resourceSpecification.resource() == null) {
			return Mono.error(new IllegalArgumentException("Resource must not be null"));
		}

		if (this.serverCapabilities.resources() == null) {
			return Mono.error(new IllegalStateException(
					"Server must be configured with resource capabilities to allow adding resources"));
		}

		return Mono.defer(() -> {
			var previous = this.resources.put(resourceSpecification.resource().uri(), resourceSpecification);
			if (previous != null) {
				logger.warn("Replace existing Resource with URI '{}'", resourceSpecification.resource().uri());
			}
			else {
				logger.debug("Added resource handler: {}", resourceSpecification.resource().uri());
			}
			if (Boolean.TRUE.equals(this.serverCapabilities.resources().listChanged())) {
				return notifyResourcesListChanged();
			}
			return Mono.empty();
		});
	}

	/**
	 * List all registered resources.
	 * @return A Flux stream of all registered resources
	 */
	public Flux<McpSchema.Resource> listResources() {
		return Flux.fromIterable(this.resources.values()).map(McpServerFeatures.AsyncResourceSpecification::resource);
	}

	/**
	 * Remove a resource handler at runtime.
	 * @param resourceUri The URI of the resource handler to remove
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> removeResource(String resourceUri) {
		if (resourceUri == null) {
			return Mono.error(new IllegalArgumentException("Resource URI must not be null"));
		}
		if (this.serverCapabilities.resources() == null) {
			return Mono.error(new IllegalStateException(
					"Server must be configured with resource capabilities to allow removing resources"));
		}

		return Mono.defer(() -> {
			McpServerFeatures.AsyncResourceSpecification removed = this.resources.remove(resourceUri);
			if (removed != null) {
				logger.debug("Removed resource handler: {}", resourceUri);
				if (Boolean.TRUE.equals(this.serverCapabilities.resources().listChanged())) {
					return notifyResourcesListChanged();
				}
				return Mono.empty();
			}
			else {
				logger.warn("Failed to remove resource with URI '{}' (not found)", resourceUri);
			}
			return Mono.empty();
		});
	}

	/**
	 * Add a new resource template at runtime.
	 * @param resourceTemplateSpecification The resource template to add
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> addResourceTemplate(
			McpServerFeatures.AsyncResourceTemplateSpecification resourceTemplateSpecification) {

		if (this.serverCapabilities.resources() == null) {
			return Mono.error(new IllegalStateException(
					"Server must be configured with resource capabilities to allow adding resource templates"));
		}

		return Mono.defer(() -> {
			var previous = this.resourceTemplates.put(resourceTemplateSpecification.resourceTemplate().uriTemplate(),
					resourceTemplateSpecification);
			if (previous != null) {
				logger.warn("Replace existing Resource Template with URI '{}'",
						resourceTemplateSpecification.resourceTemplate().uriTemplate());
			}
			else {
				logger.debug("Added resource template handler: {}",
						resourceTemplateSpecification.resourceTemplate().uriTemplate());
			}
			if (Boolean.TRUE.equals(this.serverCapabilities.resources().listChanged())) {
				return notifyResourcesListChanged();
			}
			return Mono.empty();
		});
	}

	/**
	 * List all registered resource templates.
	 * @return A Flux stream of all registered resource templates
	 */
	public Flux<McpSchema.ResourceTemplate> listResourceTemplates() {
		return Flux.fromIterable(this.resourceTemplates.values())
			.map(McpServerFeatures.AsyncResourceTemplateSpecification::resourceTemplate);
	}

	/**
	 * Remove a resource template at runtime.
	 * @param uriTemplate The URI template of the resource template to remove
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> removeResourceTemplate(String uriTemplate) {

		if (this.serverCapabilities.resources() == null) {
			return Mono.error(new IllegalStateException(
					"Server must be configured with resource capabilities to allow removing resource templates"));
		}

		return Mono.defer(() -> {
			McpServerFeatures.AsyncResourceTemplateSpecification removed = this.resourceTemplates.remove(uriTemplate);
			if (removed != null) {
				logger.debug("Removed resource template: {}", uriTemplate);
				if (Boolean.TRUE.equals(this.serverCapabilities.resources().listChanged())) {
					return notifyResourcesListChanged();
				}
				return Mono.empty();
			}
			else {
				logger.warn("Failed to remove a resource template with URI '{}' (not found)", uriTemplate);
			}
			return Mono.empty();
		});
	}

	/**
	 * Notifies clients that the list of available resources has changed.
	 * @return A Mono that completes when all clients have been notified
	 */
	public Mono<Void> notifyResourcesListChanged() {
		return this.mcpTransportProvider.notifyClients(McpSchema.METHOD_NOTIFICATION_RESOURCES_LIST_CHANGED, null);
	}

	/**
	 * Notifies only the sessions that have subscribed to the updated resource URI.
	 * @param resourcesUpdatedNotification the notification containing the updated
	 * resource URI
	 * @return A Mono that completes when all subscribed sessions have been notified
	 */
	// 资源“内容变化”按 URI 精确查订阅者并定向通知，区别于 resources/list_changed 的全客户端广播。
	// 一个会话发送失败在该分支记录并吞掉，避免阻断其他订阅者；此方法不自动侦测底层文件/数据变化。
	public Mono<Void> notifyResourcesUpdated(McpSchema.ResourcesUpdatedNotification resourcesUpdatedNotification) {
		return Mono.defer(() -> {
			String uri = resourcesUpdatedNotification.uri();
			Set<String> subscribedSessions = this.resourceSubscriptions.get(uri);
			if (subscribedSessions == null || subscribedSessions.isEmpty()) {
				logger.debug("No sessions subscribed to resource URI: {}", uri);
				return Mono.empty();
			}
			return Flux.fromIterable(subscribedSessions)
				.flatMap(sessionId -> this.mcpTransportProvider
					.notifyClient(sessionId, McpSchema.METHOD_NOTIFICATION_RESOURCES_UPDATED,
							resourcesUpdatedNotification)
					.doOnError(e -> logger.error("Failed to notify session {} of resource update for {}", sessionId,
							uri, e))
					.onErrorComplete())
				.then();
		});
	}

	// 注入 Session onClose 的业务清理钩子：注销该会话的资源订阅，防止断开客户端持续留在订阅集合中。
	// 真正的连接关闭与 Provider.sessions 的移除仍由会话/传输生命周期负责。
	private Mono<Void> cleanupForSession(String sessionId) {
		return Mono.fromRunnable(() -> {
			removeSessionSubscriptions(sessionId);
		});
	}

	private void removeSessionSubscriptions(String sessionId) {
		this.resourceSubscriptions.forEach((uri, sessions) -> sessions.remove(sessionId));
		this.resourceSubscriptions.entrySet().removeIf(entry -> entry.getValue().isEmpty());
	}

	// subscribe 能力决定是否注册本方法；这里登记 URI -> 逻辑会话 ID，而不是每条 HTTP 连接。
	// 因而 Streamable HTTP 的连接重建无需重新登记，直到 unsubscribe、DELETE 或会话关闭才清理。
	private McpRequestHandler<Object> resourcesSubscribeRequestHandler() {
		return (exchange, params) -> Mono.defer(() -> {
			McpSchema.SubscribeRequest subscribeRequest = jsonMapper.convertValue(params,
					new TypeRef<McpSchema.SubscribeRequest>() {
					});
			String uri = subscribeRequest.uri();
			String sessionId = exchange.sessionId();
			this.resourceSubscriptions.computeIfAbsent(uri, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
				.add(sessionId);
			logger.debug("Session {} subscribed to resource URI: {}", sessionId, uri);

			return Mono.just(Map.of());
		});
	}

	private McpRequestHandler<Object> resourcesUnsubscribeRequestHandler() {
		return (exchange, params) -> Mono.defer(() -> {
			McpSchema.UnsubscribeRequest unsubscribeRequest = jsonMapper.convertValue(params,
					new TypeRef<McpSchema.UnsubscribeRequest>() {
					});
			String uri = unsubscribeRequest.uri();
			String sessionId = exchange.sessionId();
			Set<String> sessions = this.resourceSubscriptions.get(uri);
			if (sessions != null) {
				sessions.remove(sessionId);
				if (sessions.isEmpty()) {
					this.resourceSubscriptions.remove(uri, sessions);
				}
			}
			logger.debug("Session {} unsubscribed from resource URI: {}", sessionId, uri);
			return Mono.just(Map.of());
		});
	}

	private McpRequestHandler<McpSchema.ListResourcesResult> resourcesListRequestHandler() {
		return (exchange, params) -> {
			var resourceList = this.resources.values()
				.stream()
				.map(McpServerFeatures.AsyncResourceSpecification::resource)
				.toList();
			return Mono.just(McpSchema.ListResourcesResult.builder(resourceList).build());
		};
	}

	private McpRequestHandler<McpSchema.ListResourceTemplatesResult> resourceTemplateListRequestHandler() {
		return (exchange, params) -> {
			var resourceList = this.resourceTemplates.values()
				.stream()
				.map(McpServerFeatures.AsyncResourceTemplateSpecification::resourceTemplate)
				.toList();
			return Mono.just(McpSchema.ListResourceTemplatesResult.builder(resourceList).build());
		};
	}

	// 先查 Resource 注册表，再查 ResourceTemplate；匹配规则统一由 URI 模板管理器提供。
	// 注册项保存“元数据 + 读取函数”，客户端 read 传入的仍是具体 URI，由业务函数决定如何读取内容。
	// 两处都不匹配才返回 RESOURCE_NOT_FOUND；多个模板重叠时不应依赖并发 Map 遍历的选择次序。
	private McpRequestHandler<McpSchema.ReadResourceResult> resourcesReadRequestHandler() {
		return (ex, params) -> {
			McpSchema.ReadResourceRequest resourceRequest = jsonMapper.convertValue(params, new TypeRef<>() {
			});

			var resourceUri = resourceRequest.uri();

			// First try to find a static resource specification
			// Static resources have exact URIs
			return this.findResourceSpecification(resourceUri)
				.map(spec -> spec.readHandler().apply(ex, resourceRequest))
				.orElseGet(() -> {
					// If not found, try to find a dynamic resource specification
					// Dynamic resources have URI templates
					return this.findResourceTemplateSpecification(resourceUri)
						.map(spec -> spec.readHandler().apply(ex, resourceRequest))
						.orElseGet(() -> Mono.error(RESOURCE_NOT_FOUND.apply(resourceUri)));
				});
		};
	}

	private Optional<McpServerFeatures.AsyncResourceSpecification> findResourceSpecification(String uri) {
		var result = this.resources.values()
			.stream()
			.filter(spec -> this.uriTemplateManagerFactory.create(spec.resource().uri()).matches(uri))
			.findFirst();
		return result;
	}

	private Optional<McpServerFeatures.AsyncResourceTemplateSpecification> findResourceTemplateSpecification(
			String uri) {
		return this.resourceTemplates.values()
			.stream()
			.filter(spec -> this.uriTemplateManagerFactory.create(spec.resourceTemplate().uriTemplate()).matches(uri))
			.findFirst();
	}

	// ---------------------------------------
	// Prompt Management
	// ---------------------------------------

	/**
	 * Add a new prompt handler at runtime.
	 * @param promptSpecification The prompt handler to add
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> addPrompt(McpServerFeatures.AsyncPromptSpecification promptSpecification) {
		if (promptSpecification == null) {
			return Mono.error(new IllegalArgumentException("Prompt specification must not be null"));
		}
		if (this.serverCapabilities.prompts() == null) {
			return Mono.error(new IllegalStateException("Server must be configured with prompt capabilities"));
		}

		return Mono.defer(() -> {
			var previous = this.prompts.put(promptSpecification.prompt().name(), promptSpecification);
			if (previous != null) {
				logger.warn("Replace existing Prompt with name '{}'", promptSpecification.prompt().name());
			}
			else {
				logger.debug("Added prompt handler: {}", promptSpecification.prompt().name());
			}
			if (Boolean.TRUE.equals(this.serverCapabilities.prompts().listChanged())) {
				return this.notifyPromptsListChanged();
			}

			return Mono.empty();
		});
	}

	/**
	 * List all registered prompts.
	 * @return A Flux stream of all registered prompts
	 */
	public Flux<McpSchema.Prompt> listPrompts() {
		return Flux.fromIterable(this.prompts.values()).map(McpServerFeatures.AsyncPromptSpecification::prompt);
	}

	/**
	 * Remove a prompt handler at runtime.
	 * @param promptName The name of the prompt handler to remove
	 * @return Mono that completes when clients have been notified of the change
	 */
	public Mono<Void> removePrompt(String promptName) {
		if (promptName == null) {
			return Mono.error(new IllegalArgumentException("Prompt name must not be null"));
		}
		if (this.serverCapabilities.prompts() == null) {
			return Mono.error(new IllegalStateException("Server must be configured with prompt capabilities"));
		}

		return Mono.defer(() -> {
			McpServerFeatures.AsyncPromptSpecification removed = this.prompts.remove(promptName);

			if (removed != null) {
				logger.debug("Removed prompt handler: {}", promptName);
				if (Boolean.TRUE.equals(this.serverCapabilities.prompts().listChanged())) {
					return this.notifyPromptsListChanged();
				}
				return Mono.empty();
			}
			else {
				logger.warn("Failed to remove a prompt with name '{}' (not found)", promptName);
			}
			return Mono.empty();
		});
	}

	/**
	 * Notifies clients that the list of available prompts has changed.
	 * @return A Mono that completes when all clients have been notified
	 */
	public Mono<Void> notifyPromptsListChanged() {
		return this.mcpTransportProvider.notifyClients(McpSchema.METHOD_NOTIFICATION_PROMPTS_LIST_CHANGED, null);
	}

	/**
	 * Sends an elicitation complete notification to a specific client session, indicating
	 * that an out-of-band URL elicitation interaction has completed.
	 * @param sessionId The ID of the session to notify
	 * @param notification The notification containing the elicitation ID
	 * @return A Mono that completes when the notification has been sent
	 */
	public Mono<Void> sendElicitationComplete(String sessionId,
			McpSchema.ElicitationCompleteNotification notification) {
		if (sessionId == null) {
			return Mono.error(new IllegalArgumentException("Session ID must not be null"));
		}
		if (notification == null) {
			return Mono.error(new IllegalArgumentException("Notification must not be null"));
		}
		return this.mcpTransportProvider.notifyClient(sessionId, McpSchema.METHOD_NOTIFICATION_ELICITATION_COMPLETE,
				notification);
	}

	private McpRequestHandler<McpSchema.ListPromptsResult> promptsListRequestHandler() {
		return (exchange, params) -> {
			// TODO: Implement pagination
			// McpSchema.PaginatedRequest request = objectMapper.convertValue(params,
			// new TypeReference<McpSchema.PaginatedRequest>() {
			// });

			var promptList = this.prompts.values()
				.stream()
				.map(McpServerFeatures.AsyncPromptSpecification::prompt)
				.toList();

			return Mono.just(McpSchema.ListPromptsResult.builder(promptList).build());
		};
	}

	// Prompt 注册表按名称查找，handler 接收完整 GetPromptRequest 并负责生成消息列表。
	// 不存在的名称是 INVALID_PARAMS；这里没有工具输入那样的 JSON Schema 参数校验。
	private McpRequestHandler<McpSchema.GetPromptResult> promptsGetRequestHandler() {
		return (exchange, params) -> {
			McpSchema.GetPromptRequest promptRequest = jsonMapper.convertValue(params,
					new TypeRef<McpSchema.GetPromptRequest>() {
					});

			// Implement prompt retrieval logic here
			McpServerFeatures.AsyncPromptSpecification specification = this.prompts.get(promptRequest.name());

			if (specification == null) {
				return Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS)
					.message("Invalid prompt name")
					.data("Prompt not found: " + promptRequest.name())
					.build());
			}

			return Mono.defer(() -> specification.promptHandler().apply(exchange, promptRequest));
		};
	}

	// ---------------------------------------
	// Logging Management
	// ---------------------------------------

	private McpRequestHandler<Object> setLoggerRequestHandler() {
		return (exchange, params) -> {
			return Mono.defer(() -> {

				SetLevelRequest newMinLoggingLevel = jsonMapper.convertValue(params, new TypeRef<SetLevelRequest>() {
				});

				exchange.setMinLoggingLevel(newMinLoggingLevel.level());

				return Mono.just(Map.of());
			});
		};
	}

	private static final Mono<McpSchema.CompleteResult> EMPTY_COMPLETION_RESULT = Mono
		.just(new McpSchema.CompleteResult(new CompleteCompletion(List.of(), 0, false)));

	// 补全先校验引用的 Prompt/Resource 及参数名，再按 CompleteReference 查用户注册的补全函数。
	// 返回空补全集合并不总是错误：例如固定资源 URI 或没有注册补全 handler 时返回 EMPTY_COMPLETION_RESULT。
	private McpRequestHandler<McpSchema.CompleteResult> completionCompleteRequestHandler() {
		return (exchange, params) -> {

			McpSchema.CompleteRequest request = jsonMapper.convertValue(params, new TypeRef<>() {
			});

			if (request.ref() == null) {
				return Mono.error(
						McpError.builder(ErrorCodes.INVALID_PARAMS).message("Completion ref must not be null").build());
			}

			if (request.ref().type() == null) {
				return Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS)
					.message("Completion ref type must not be null")
					.build());
			}

			String type = request.ref().type();

			String argumentName = request.argument().name();

			// Check if valid a Prompt exists for this completion request
			if (type.equals(PromptReference.TYPE)
					&& request.ref() instanceof McpSchema.PromptReference promptReference) {

				McpServerFeatures.AsyncPromptSpecification promptSpec = this.prompts.get(promptReference.name());
				if (promptSpec == null) {
					return Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS)
						.message("Prompt not found: " + promptReference.name())
						.build());
				}
				List<McpSchema.PromptArgument> arguments = promptSpec.prompt().arguments();
				if (arguments == null
						|| !arguments.stream().filter(arg -> arg.name().equals(argumentName)).findFirst().isPresent()) {

					logger.warn("Argument not found: {} in prompt: {}", argumentName, promptReference.name());

					return EMPTY_COMPLETION_RESULT;
				}
			}

			// Check if valid Resource or ResourceTemplate exists for this completion
			// request
			if (type.equals(ResourceReference.TYPE)
					&& request.ref() instanceof McpSchema.ResourceReference resourceReference) {

				var uriTemplateManager = uriTemplateManagerFactory.create(resourceReference.uri());

				if (!uriTemplateManager.isUriTemplate(resourceReference.uri())) {
					// Attempting to autocomplete a fixed resource URI is not an error in
					// the spec (but probably should be).
					return EMPTY_COMPLETION_RESULT;
				}

				McpServerFeatures.AsyncResourceSpecification resourceSpec = this
					.findResourceSpecification(resourceReference.uri())
					.orElse(null);

				if (resourceSpec != null) {
					if (!uriTemplateManagerFactory.create(resourceSpec.resource().uri())
						.getVariableNames()
						.contains(argumentName)) {

						return Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS)
							.message("Argument not found: " + argumentName + " in resource: " + resourceReference.uri())
							.build());
					}
				}
				else {
					var templateSpec = this.findResourceTemplateSpecification(resourceReference.uri()).orElse(null);
					if (templateSpec != null) {

						if (!uriTemplateManagerFactory.create(templateSpec.resourceTemplate().uriTemplate())
							.getVariableNames()
							.contains(argumentName)) {

							return Mono.error(McpError.builder(ErrorCodes.INVALID_PARAMS)
								.message("Argument not found: " + argumentName + " in resource template: "
										+ resourceReference.uri())
								.build());
						}
					}
					else {
						return Mono.error(RESOURCE_NOT_FOUND.apply(resourceReference.uri()));
					}
				}
			}

			// Handle the completion request using the registered handler
			// for the given reference.
			McpServerFeatures.AsyncCompletionSpecification specification = this.completions.get(request.ref());

			if (specification == null) {
				return EMPTY_COMPLETION_RESULT;
			}

			return Mono.defer(() -> specification.completionHandler().apply(exchange, request));
		};
	}

	/**
	 * This method is package-private and used for test only. Should not be called by user
	 * code.
	 * @param protocolVersions the Client supported protocol versions.
	 */
	void setProtocolVersions(List<String> protocolVersions) {
		this.protocolVersions = protocolVersions;
	}

}
