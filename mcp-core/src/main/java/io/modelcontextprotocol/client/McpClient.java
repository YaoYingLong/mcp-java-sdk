/*
 * Copyright 2024-2026 the original author or authors.
 */

package io.modelcontextprotocol.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.spec.McpSchema.CreateMessageRequest;
import io.modelcontextprotocol.spec.McpSchema.CreateMessageResult;
import io.modelcontextprotocol.spec.McpSchema.ElicitFormRequest;
import io.modelcontextprotocol.spec.McpSchema.ElicitResult;
import io.modelcontextprotocol.spec.McpSchema.ElicitUrlRequest;
import io.modelcontextprotocol.spec.McpSchema.Implementation;
import io.modelcontextprotocol.spec.McpSchema.Root;
import io.modelcontextprotocol.spec.McpTransport;
import io.modelcontextprotocol.util.Assert;
import reactor.core.publisher.Mono;

/**
 * Factory class for creating Model Context Protocol (MCP) clients. MCP is a protocol that
 * enables AI models to interact with external tools and resources through a standardized
 * interface.
 *
 * <p>
 * This class serves as the main entry point for establishing connections with MCP
 * servers, implementing the client-side of the MCP specification. The protocol follows a
 * client-server architecture where:
 * <ul>
 * <li>The client (this implementation) initiates connections and sends requests
 * <li>The server responds to requests and provides access to tools and resources
 * <li>Communication occurs through a transport layer (e.g., stdio, SSE) using JSON-RPC
 * 2.0
 * </ul>
 *
 * <p>
 * The class provides factory methods to create either:
 * <ul>
 * <li>{@link McpAsyncClient} for non-blocking operations with Mono/Flux responses
 * <li>{@link McpSyncClient} for blocking operations with direct responses
 * </ul>
 *
 * <p>
 * Example of creating a basic synchronous client: <pre>{@code
 * McpClient.sync(transport)
 *     .requestTimeout(Duration.ofSeconds(5))
 *     .build();
 * }</pre>
 *
 * Example of creating a basic asynchronous client: <pre>{@code
 * McpClient.async(transport)
 *     .requestTimeout(Duration.ofSeconds(5))
 *     .build();
 * }</pre>
 *
 * <p>
 * Example with advanced asynchronous configuration: <pre>{@code
 * McpClient.async(transport)
 *     .requestTimeout(Duration.ofSeconds(10))
 *     .capabilities(new ClientCapabilities(...))
 *     .clientInfo(new Implementation("My Client", "1.0.0"))
 *     .roots(new Root("file://workspace", "Workspace Files"))
 *     .toolsChangeConsumer(tools -> Mono.fromRunnable(() -> System.out.println("Tools updated: " + tools)))
 *     .resourcesChangeConsumer(resources -> Mono.fromRunnable(() -> System.out.println("Resources updated: " + resources)))
 *     .promptsChangeConsumer(prompts -> Mono.fromRunnable(() -> System.out.println("Prompts updated: " + prompts)))
 *     .loggingConsumer(message -> Mono.fromRunnable(() -> System.out.println("Log message: " + message)))
 *     .resourcesUpdateConsumer(resourceContents -> Mono.fromRunnable(() -> System.out.println("Resources contents updated: " + resourceContents)))
 *     .build();
 * }</pre>
 *
 * <p>
 * The client supports:
 * <ul>
 * <li>Tool discovery and invocation
 * <li>Resource access and management
 * <li>Prompt template handling
 * <li>Real-time updates through change consumers
 * <li>Custom sampling strategies
 * <li>Structured logging with severity levels
 * </ul>
 *
 * <p>
 * The client supports structured logging through the MCP logging utility:
 * <ul>
 * <li>Eight severity levels from DEBUG to EMERGENCY
 * <li>Optional logger name categorization
 * <li>Configurable logging consumers
 * <li>Server-controlled minimum log level
 * </ul>
 *
 * @author Christian Tzolov
 * @author Dariusz Jędrzejczyk
 * @author Anurag Pant
 * @see McpAsyncClient
 * @see McpSyncClient
 * @see McpTransport
 */
// 中文阅读入口：这里负责收集配置，不直接实现 JSON-RPC。
// sync/async 两条构建路径最终都创建 McpAsyncClient；同步客户端只是其阻塞门面。
// requestTimeout 交给 McpClientSession 约束单次 RPC，initializationTimeout 则交给
// LifecycleInitializer 约束等待初始化的阶段；传输自己的 connectTimeout 又是另一层配置。
public interface McpClient {

	/**
	 * Start building a synchronous MCP client with the specified transport layer. The
	 * synchronous MCP client provides blocking operations. Synchronous clients wait for
	 * each operation to complete before returning, making them simpler to use but
	 * potentially less performant for concurrent operations. The transport layer handles
	 * the low-level communication between client and server using protocols like stdio or
	 * Server-Sent Events (SSE).
	 * @param transport The transport layer implementation for MCP communication. Common
	 * implementations include {@code StdioClientTransport} for stdio-based communication
	 * and {@code SseClientTransport} for SSE-based communication.
	 * @return A new builder instance for configuring the client
	 * @throws IllegalArgumentException if transport is null
	 */
	static SyncSpec sync(McpClientTransport transport) {
		// 第一步只创建配置构建器并保存 transport；链式方法收集配置，build() 才创建客户端。
		return new SyncSpec(transport);
	}

	/**
	 * Start building an asynchronous MCP client with the specified transport layer. The
	 * asynchronous MCP client provides non-blocking operations. Asynchronous clients
	 * return reactive primitives (Mono/Flux) immediately, allowing for concurrent
	 * operations and reactive programming patterns. The transport layer handles the
	 * low-level communication between client and server using protocols like stdio or
	 * Server-Sent Events (SSE).
	 * @param transport The transport layer implementation for MCP communication. Common
	 * implementations include {@code StdioClientTransport} for stdio-based communication
	 * and {@code SseClientTransport} for SSE-based communication.
	 * @return A new builder instance for configuring the client
	 * @throws IllegalArgumentException if transport is null
	 */
	static AsyncSpec async(McpClientTransport transport) {
		// 配置阶段与 sync 相同；区别在于回调返回 Mono，build() 直接得到 McpAsyncClient。
		return new AsyncSpec(transport);
	}

	/**
	 * Synchronous client specification. This class follows the builder pattern to provide
	 * a fluent API for setting up clients with custom configurations.
	 *
	 * <p>
	 * The builder supports configuration of:
	 * <ul>
	 * <li>Transport layer for client-server communication
	 * <li>Request timeouts for operation boundaries
	 * <li>Client capabilities for feature negotiation
	 * <li>Client implementation details for version tracking
	 * <li>Root URIs for resource access
	 * <li>Change notification handlers for tools, resources, and prompts
	 * <li>Custom message sampling logic
	 * </ul>
	 */
	class SyncSpec {

		// 可把 Spec 理解为“创建客户端前的配置清单”：普通 setter 覆盖字段并返回 this，
		// roots 按 URI 合并，Consumer 方法向列表追加；配置这些回调时并不会执行它们。
		// build() 将字段装配到 McpClientFeatures.Sync，再适配为 Async 并创建同步客户端门面。
		// 同步业务调用会阻塞等待结果；这里登记的同步回调则由 fromSync 调度到 boundedElastic。

		/**
		 * 消息收发通道，由 sync(transport) 必传，构建后交给内部 McpAsyncClient 使用。 例如 stdio 管理子进程输入输出，HTTP
		 * transport 管理请求与 SSE；连接地址、认证等在各自 transport 的 builder 上配置。客户端关闭时也会关闭此 transport。
		 */
		private final McpClientTransport transport;

		/**
		 * 单次出站 JSON-RPC 请求等待响应的上限，默认 20 秒，传入 McpClientSession。 initialize
		 * 请求也受此限制；通知不等待响应，不适用这个超时。 超时终止本地等待，不保证远端工具已停止，也不是 HTTP 建连超时。
		 */
		private Duration requestTimeout = Duration.ofSeconds(20); // Default timeout

		/**
		 * 等待初始化完成的上限，默认 20 秒，传入 LifecycleInitializer。 包括握手与初始化后置工作（例如开启 schema
		 * 缓存后的工具列表查询），不包含随后业务调用。 与 requestTimeout 独立：初始化里的每次 RPC 仍受单次请求超时约束。
		 */
		private Duration initializationTimeout = Duration.ofSeconds(20);

		/**
		 * initialize 时向服务端声明“本客户端能处理什么”，如 roots、sampling、elicitation。 保持 null 时，build()
		 * 根据已配置的 roots 和 handler 推导；显式设置后使用该对象，不再合并推导。 服务端的 tools/resources/prompts 能力从
		 * InitializeResult 获取，不由此字段开启。 声明 elicitation 能力时应同时提供对应 form/url handler。
		 */
		private ClientCapabilities capabilities;

		/**
		 * 随 initialize 发给服务端的客户端名称、版本等展示信息，便于识别调用方实现和排查问题。 下面的 0.15.0
		 * 是当前代码中的默认实现版本字符串，不是协商的 MCP 协议版本，也不提供身份认证。
		 */
		private Implementation clientInfo = Implementation.builder("Java SDK MCP Client", "0.15.0").build();

		/**
		 * 向服务端 roots/list 请求返回的根 URI 集合，例如用户提供的工作目录；键为 Root.uri()。 默认空，重复配置同一 URI
		 * 会覆盖该项；这里只声明范围，不读取文件或实施文件系统权限控制。 未显式指定 capabilities 且集合非空时，推导 roots
		 * 能力，listChanged 默认 false。 运行期用客户端 addRoot/removeRoot 更新；需要自动发送变更通知时显式声明
		 * roots(true)。
		 */
		private final Map<String, Root> roots = new HashMap<>();

		/**
		 * 工具目录变更监听器，默认空。收到 notifications/tools/list_changed 后，SDK 先重新 listTools，
		 * 再把查询得到的完整工具定义列表交给各 Consumer；参数不是增量，也不是工具执行结果。
		 */
		private final List<Consumer<List<McpSchema.Tool>>> toolsChangeConsumers = new ArrayList<>();

		/**
		 * 资源目录变更监听器，默认空。收到 notifications/resources/list_changed 后先重新 listResources，
		 * 再传入资源元数据列表，适合刷新资源选择界面；不包含资源正文。
		 */
		private final List<Consumer<List<McpSchema.Resource>>> resourcesChangeConsumers = new ArrayList<>();

		/**
		 * 单个资源内容更新监听器，默认空。收到 notifications/resources/updated 后，SDK 按通知中的 URI 执行
		 * readResource，再把 contents 列表交给 Consumer。注册监听器本身不发送 resources/subscribe；
		 * 需要服务端按订阅推送时，另调用客户端 subscribeResource。
		 */
		private final List<Consumer<List<McpSchema.ResourceContents>>> resourcesUpdateConsumers = new ArrayList<>();

		/**
		 * 提示词目录变更监听器，默认空。收到 notifications/prompts/list_changed 后重新 listPrompts，
		 * 回调得到提示词定义列表；具体提示词展开后的消息仍需通过 getPrompt 获取。
		 */
		private final List<Consumer<List<McpSchema.Prompt>>> promptsChangeConsumers = new ArrayList<>();

		/**
		 * 服务端 notifications/message 日志的接收器，默认空；可读取 level、logger 和 data。 该配置不设置 SDK
		 * 自身的日志级别；要求服务端调整日志阈值时使用客户端 setLoggingLevel。
		 */
		private final List<Consumer<McpSchema.LoggingMessageNotification>> loggingConsumers = new ArrayList<>();

		/**
		 * 服务端 notifications/progress 进度通知的接收器，默认空，参数含 progressToken、progress、total 等。
		 * 注册不会自动为请求生成 progressToken；调用方在请求 _meta 中设置 token，并在回调中据此区分任务。
		 */
		private final List<Consumer<McpSchema.ProgressNotification>> progressConsumers = new ArrayList<>();

		/**
		 * URL 引导流程完成通知的接收器，默认空。收到 notifications/elicitation/complete 时调用， 可按
		 * elicitationId 更新先前交互的状态；它与立即回答 URL 引导请求的 urlElicitationHandler 分工不同。
		 */
		private final List<Consumer<McpSchema.ElicitationCompleteNotification>> elicitationCompleteConsumers = new ArrayList<>();

		/**
		 * 处理服务端反向发来的 sampling/createMessage 请求，默认 null。请求带 messages、模型偏好等，
		 * 应用在此决定是否允许，并调用自己接入的模型，返回 CreateMessageResult；SDK 不代为选择模型服务。 未显式指定 capabilities
		 * 时，配置此函数会推导 sampling 能力。
		 */
		private Function<CreateMessageRequest, CreateMessageResult> samplingHandler;

		/**
		 * 处理服务端 form 模式的 elicitation/create 请求，默认 null，通过 elicitation(...) 配置。 应用根据
		 * message/requestedSchema 收集用户输入，返回 ACCEPT/DECLINE/CANCEL 及相应 content； SDK 不提供表单
		 * UI。未显式指定 capabilities 时，配置此函数会推导 form elicitation 能力。
		 */
		private Function<ElicitFormRequest, ElicitResult> formElicitationHandler;

		/**
		 * 处理服务端 URL 模式的 elicitation/create 请求，默认 null，通过 urlElicitation(...) 配置。 应用根据请求中的
		 * url/message/elicitationId 引导用户并返回交互决定；SDK 不自动打开浏览器。 未显式指定 capabilities 时推导 URL
		 * 能力；外部流程最终完成可由 elicitationCompleteConsumers 接收。
		 */
		private Function<ElicitUrlRequest, ElicitResult> urlElicitationHandler;

		/**
		 * 同步调用的本地传输上下文提供器，默认返回 EMPTY，通过 transportContextProvider(...) 设置。
		 * 需要传输的操作在调用线程订阅时取值，可从 ThreadLocal 提取身份等信息并写入 Reactor Context， 供后续 HTTP
		 * 定制器等读取；不会自动变成请求 _meta 或 HTTP 头，也不会在 build() 时固定取值。
		 */
		private Supplier<McpTransportContext> contextProvider = () -> McpTransportContext.EMPTY;

		/**
		 * 工具结构化输出的 JSON Schema 校验实现。默认 null，build() 从 McpJsonDefaults 获取默认 provider。
		 * callTool 的输出校验还需开启 enableCallToolSchemaCaching 且缓存中有 outputSchema； 仅指定
		 * validator 不会开启该功能，也不替代 transport 的 JSON 序列化器。
		 */
		private JsonSchemaValidator jsonSchemaValidator;

		/**
		 * 是否缓存工具 outputSchema 并校验 callTool 的 structuredContent，默认 false，不缓存工具执行结果。
		 * 开启后，服务端有 tools 能力时初始化会查询第一页工具；后续 listTools 查询到的 schema 也会写入缓存。 当前 callTool
		 * 遇到缓存缺失仅告警并返回结果，不自动补查列表；isError=true 的结果跳过输出校验。
		 */
		private boolean enableCallToolSchemaCaching = false; // Default to false

		/**
		 * 是否为表单引导结果补充 schema 默认值，默认 false，仅影响本地处理，不写入能力声明。 只有 form handler 返回 ACCEPT 且
		 * content 非 null 时，复制 content 并补齐顶层缺失字段的 default。 已存在的键（即使值为
		 * null）不覆盖，不递归处理嵌套字段，也不处理 URL 模式或拒绝/取消结果。
		 */
		private boolean applyElicitationDefaults = false; // Default to false

		private SyncSpec(McpClientTransport transport) {
			Assert.notNull(transport, "Transport must not be null");
			this.transport = transport;
		}

		/**
		 * Sets the duration to wait for server responses before timing out requests. This
		 * timeout applies to all requests made through the client, including tool calls,
		 * resource access, and prompt operations.
		 * @param requestTimeout The duration to wait before timing out requests. Must not
		 * be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if requestTimeout is null
		 */
		public SyncSpec requestTimeout(Duration requestTimeout) {
			Assert.notNull(requestTimeout, "Request timeout must not be null");
			this.requestTimeout = requestTimeout;
			return this;
		}

		/**
		 * @param initializationTimeout The duration to wait for the initialization
		 * lifecycle step to complete.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if initializationTimeout is null
		 */
		public SyncSpec initializationTimeout(Duration initializationTimeout) {
			Assert.notNull(initializationTimeout, "Initialization timeout must not be null");
			this.initializationTimeout = initializationTimeout;
			return this;
		}

		/**
		 * Sets the client capabilities that will be advertised to the server during
		 * connection initialization. Capabilities describe client-side features such as
		 * roots, sampling, and elicitation; server capabilities are negotiated
		 * separately.
		 * @param capabilities The client capabilities configuration. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if capabilities is null
		 */
		public SyncSpec capabilities(ClientCapabilities capabilities) {
			Assert.notNull(capabilities, "Capabilities must not be null");
			this.capabilities = capabilities;
			return this;
		}

		/**
		 * Sets the client implementation information that will be shared with the server
		 * during connection initialization. This helps with version compatibility and
		 * debugging.
		 * @param clientInfo The client implementation details including name and version.
		 * Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if clientInfo is null
		 */
		public SyncSpec clientInfo(Implementation clientInfo) {
			Assert.notNull(clientInfo, "Client info must not be null");
			this.clientInfo = clientInfo;
			return this;
		}

		/**
		 * Sets the root URIs that this client can access. Roots define the base URIs for
		 * resources that the client can request from the server. For example, a root
		 * might be "file://workspace" for accessing workspace files.
		 * <p>
		 * Roots are deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * answering {@code roots/list} for servers that still use roots.
		 * @param roots A list of root definitions. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if roots is null
		 */
		public SyncSpec roots(List<Root> roots) {
			Assert.notNull(roots, "Roots must not be null");
			for (Root root : roots) {
				this.roots.put(root.uri(), root);
			}
			return this;
		}

		/**
		 * Sets the root URIs that this client can access, using a varargs parameter for
		 * convenience. This is an alternative to {@link #roots(List)}.
		 * <p>
		 * Roots are deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * answering {@code roots/list} for servers that still use roots.
		 * @param roots An array of root definitions. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if roots is null
		 * @see #roots(List)
		 */
		public SyncSpec roots(Root... roots) {
			Assert.notNull(roots, "Roots must not be null");
			for (Root root : roots) {
				this.roots.put(root.uri(), root);
			}
			return this;
		}

		/**
		 * Sets a handler for sampling/createMessage requests received from the server.
		 * The application invokes its model and returns the generated message result.
		 * <p>
		 * Sampling is deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * handling {@code sampling/createMessage} for servers that still use sampling.
		 * @param samplingHandler A function that processes message requests and returns
		 * results. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if samplingHandler is null
		 */
		public SyncSpec sampling(Function<CreateMessageRequest, CreateMessageResult> samplingHandler) {
			Assert.notNull(samplingHandler, "Sampling handler must not be null");
			this.samplingHandler = samplingHandler;
			return this;
		}

		/**
		 * Sets a handler for form-mode elicitation requests received from the server. The
		 * application collects user input and returns an elicitation result.
		 * @param elicitationHandler A function that processes elicitation requests and
		 * returns results. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationHandler is null
		 */
		public SyncSpec elicitation(Function<ElicitFormRequest, ElicitResult> elicitationHandler) {
			Assert.notNull(elicitationHandler, "Elicitation handler must not be null");
			this.formElicitationHandler = elicitationHandler;
			return this;
		}

		/**
		 * Sets a handler for URL-mode elicitation requests received from the server. The
		 * application guides the user to the supplied URL and returns their decision.
		 * @param elicitationHandler A function that processes elicitation requests and
		 * returns results. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationHandler is null
		 */
		public SyncSpec urlElicitation(Function<ElicitUrlRequest, ElicitResult> elicitationHandler) {
			Assert.notNull(elicitationHandler, "Elicitation handler must not be null");
			this.urlElicitationHandler = elicitationHandler;
			return this;
		}

		/**
		 * Adds a consumer to be notified when the available tools change. This allows the
		 * client to react to changes in the server's tool capabilities, such as tools
		 * being added or removed.
		 * @param toolsChangeConsumer A consumer that receives the updated list of
		 * available tools. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if toolsChangeConsumer is null
		 */
		public SyncSpec toolsChangeConsumer(Consumer<List<McpSchema.Tool>> toolsChangeConsumer) {
			Assert.notNull(toolsChangeConsumer, "Tools change consumer must not be null");
			this.toolsChangeConsumers.add(toolsChangeConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when the available resources change. This allows
		 * the client to react to changes in the server's resource availability, such as
		 * files being added or removed.
		 * @param resourcesChangeConsumer A consumer that receives the updated list of
		 * available resources. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if resourcesChangeConsumer is null
		 */
		public SyncSpec resourcesChangeConsumer(Consumer<List<McpSchema.Resource>> resourcesChangeConsumer) {
			Assert.notNull(resourcesChangeConsumer, "Resources change consumer must not be null");
			this.resourcesChangeConsumers.add(resourcesChangeConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when a specific resource is updated. This allows
		 * the client to react to changes in individual resources, such as updates to
		 * their content or metadata.
		 * @param resourcesUpdateConsumer A consumer of the updated resource contents.
		 * Must not be null.
		 * @return This builder instance for method chaining.
		 * @throws IllegalArgumentException If the resourcesUpdateConsumer is null.
		 */
		public SyncSpec resourcesUpdateConsumer(Consumer<List<McpSchema.ResourceContents>> resourcesUpdateConsumer) {
			Assert.notNull(resourcesUpdateConsumer, "Resources update consumer must not be null");
			this.resourcesUpdateConsumers.add(resourcesUpdateConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when the available prompts change. This allows
		 * the client to react to changes in the server's prompt templates, such as new
		 * templates being added or existing ones being modified.
		 * @param promptsChangeConsumer A consumer that receives the updated list of
		 * available prompts. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if promptsChangeConsumer is null
		 */
		public SyncSpec promptsChangeConsumer(Consumer<List<McpSchema.Prompt>> promptsChangeConsumer) {
			Assert.notNull(promptsChangeConsumer, "Prompts change consumer must not be null");
			this.promptsChangeConsumers.add(promptsChangeConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when logging messages are received from the
		 * server. This allows the client to react to log messages, such as warnings or
		 * errors, that are sent by the server.
		 * <p>
		 * Logging is deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * handling log messages from servers that still send them.
		 * @param loggingConsumer A consumer that receives logging messages. Must not be
		 * null.
		 * @return This builder instance for method chaining
		 */
		public SyncSpec loggingConsumer(Consumer<McpSchema.LoggingMessageNotification> loggingConsumer) {
			Assert.notNull(loggingConsumer, "Logging consumer must not be null");
			this.loggingConsumers.add(loggingConsumer);
			return this;
		}

		/**
		 * Adds multiple consumers to be notified when logging messages are received from
		 * the server. This allows the client to react to log messages, such as warnings
		 * or errors, that are sent by the server.
		 * <p>
		 * Logging is deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * handling log messages from servers that still send them.
		 * @param loggingConsumers A list of consumers that receive logging messages. Must
		 * not be null.
		 * @return This builder instance for method chaining
		 */
		public SyncSpec loggingConsumers(List<Consumer<McpSchema.LoggingMessageNotification>> loggingConsumers) {
			Assert.notNull(loggingConsumers, "Logging consumers must not be null");
			this.loggingConsumers.addAll(loggingConsumers);
			return this;
		}

		/**
		 * Adds a consumer to be notified of progress notifications from the server. This
		 * allows the client to track long-running operations and provide feedback to
		 * users.
		 * @param progressConsumer A consumer that receives progress notifications. Must
		 * not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if progressConsumer is null
		 */
		public SyncSpec progressConsumer(Consumer<McpSchema.ProgressNotification> progressConsumer) {
			Assert.notNull(progressConsumer, "Progress consumer must not be null");
			this.progressConsumers.add(progressConsumer);
			return this;
		}

		/**
		 * Adds a multiple consumers to be notified of progress notifications from the
		 * server. This allows the client to track long-running operations and provide
		 * feedback to users.
		 * @param progressConsumers A list of consumers that receives progress
		 * notifications. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if progressConsumer is null
		 */
		public SyncSpec progressConsumers(List<Consumer<McpSchema.ProgressNotification>> progressConsumers) {
			Assert.notNull(progressConsumers, "Progress consumers must not be null");
			this.progressConsumers.addAll(progressConsumers);
			return this;
		}

		/**
		 * Adds a consumer to be notified by the server when an URL elicitation is
		 * complete.
		 * @param elicitationCompleteConsumer A consumer that receives elicitation
		 * complete notifications. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationCompleteConsumer is null
		 */
		public SyncSpec elicitationCompleteConsumer(
				Consumer<McpSchema.ElicitationCompleteNotification> elicitationCompleteConsumer) {
			Assert.notNull(elicitationCompleteConsumer, "Elicitation complete consumer must not be null");
			this.elicitationCompleteConsumers.add(elicitationCompleteConsumer);
			return this;
		}

		/**
		 * Adds multiple consumers to be notified by the server when an URL elicitation is
		 * complete.
		 * @param elicitationCompleteConsumers A list of consumers that receives
		 * elicitation complete notifications. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationCompleteConsumers is null
		 */
		public SyncSpec elicitationCompleteConsumers(
				List<Consumer<McpSchema.ElicitationCompleteNotification>> elicitationCompleteConsumers) {
			Assert.notNull(elicitationCompleteConsumers, "Elicitation complete consumers must not be null");
			this.elicitationCompleteConsumers.addAll(elicitationCompleteConsumers);
			return this;
		}

		/**
		 * Add a provider of {@link McpTransportContext}, providing a context before
		 * calling any client operation. This allows to extract thread-locals and hand
		 * them over to the underlying transport.
		 * <p>
		 * The supplier is invoked at subscription time, on the calling thread, and the
		 * resulting context is visible to the transport for every leg of the operation,
		 * including connections opened on other threads, such as the SSE stream started
		 * during initialization.
		 * @param contextProvider A supplier to create a context
		 * @return This builder for method chaining
		 */
		public SyncSpec transportContextProvider(Supplier<McpTransportContext> contextProvider) {
			this.contextProvider = contextProvider;
			return this;
		}

		/**
		 * Add a {@link JsonSchemaValidator} to validate the JSON structure of the
		 * structured output.
		 * @param jsonSchemaValidator A validator to validate the JSON structure of the
		 * structured output. Must not be null.
		 * @return This builder for method chaining
		 * @throws IllegalArgumentException if jsonSchemaValidator is null
		 */
		public SyncSpec jsonSchemaValidator(JsonSchemaValidator jsonSchemaValidator) {
			Assert.notNull(jsonSchemaValidator, "JsonSchemaValidator must not be null");
			this.jsonSchemaValidator = jsonSchemaValidator;
			return this;
		}

		/**
		 * Enables output-schema caching during initialization and listTools requests, and
		 * validation of successful callTool results against cached schemas.
		 * Initialization fetches the first page; callTool does not fetch missing schemas.
		 * @param enableCallToolSchemaCaching true to enable, false to disable
		 * @return This builder instance for method chaining
		 */
		public SyncSpec enableCallToolSchemaCaching(boolean enableCallToolSchemaCaching) {
			this.enableCallToolSchemaCaching = enableCallToolSchemaCaching;
			return this;
		}

		/**
		 * Enables SDK-side merging of elicitation schema defaults into an accepted
		 * {@link ElicitResult}'s {@code content} for fields the elicitation handler left
		 * unset. This is a client-local behavior and is NOT serialized as part of the MCP
		 * capability handshake.
		 * @param applyElicitationDefaults true to enable, false to disable
		 * @return This builder instance for method chaining
		 */
		public SyncSpec applyElicitationDefaults(boolean applyElicitationDefaults) {
			this.applyElicitationDefaults = applyElicitationDefaults;
			return this;
		}

		/**
		 * Create an instance of {@link McpSyncClient} with the provided configurations or
		 * sensible defaults.
		 * @return a new instance of {@link McpSyncClient}.
		 */
		public McpSyncClient build() {
			// 用户配置的 Consumer/Function 先收进同步特性，再由 fromSync 包装成 Mono，
			// 并把这些可能阻塞的回调安排到 boundedElastic；不是把整个客户端切到该线程池。
			McpClientFeatures.Sync syncFeatures = new McpClientFeatures.Sync(this.clientInfo, this.capabilities,
					this.roots, this.toolsChangeConsumers, this.resourcesChangeConsumers, this.resourcesUpdateConsumers,
					this.promptsChangeConsumers, this.loggingConsumers, this.progressConsumers,
					this.elicitationCompleteConsumers, this.samplingHandler, this.formElicitationHandler,
					this.urlElicitationHandler, this.enableCallToolSchemaCaching, this.applyElicitationDefaults);

			McpClientFeatures.Async asyncFeatures = McpClientFeatures.Async.fromSync(syncFeatures);

			return new McpSyncClient(new McpAsyncClient(transport, this.requestTimeout, this.initializationTimeout,
					jsonSchemaValidator != null ? jsonSchemaValidator : McpJsonDefaults.getSchemaValidator(),
					asyncFeatures), this.contextProvider);
		}

	}

	/**
	 * Asynchronous client specification. This class follows the builder pattern to
	 * provide a fluent API for setting up clients with custom configurations.
	 *
	 * <p>
	 * The builder supports configuration of:
	 * <ul>
	 * <li>Transport layer for client-server communication
	 * <li>Request timeouts for operation boundaries
	 * <li>Client capabilities for feature negotiation
	 * <li>Client implementation details for version tracking
	 * <li>Root URIs for resource access
	 * <li>Change notification handlers for tools, resources, and prompts
	 * <li>Custom message sampling logic
	 * </ul>
	 *
	 * <p>
	 * Unlike {@link SyncSpec}, this specification has no
	 * {@code transportContextProvider}. This is deliberate: in a reactive pipeline the
	 * caller owns the Reactor context, and whatever the transport needs, such as an
	 * {@link McpTransportContext}, can be written into it directly. Write it once where
	 * the reactive chain starts, rather than at every call site, and every
	 * {@link McpAsyncClient} call downstream inherits it, including the connections
	 * opened during initialization: <pre>{@code
	 * chain.filter(exchange)
	 *     .contextWrite(ctx -> ctx.put(McpTransportContext.KEY, context));
	 * }</pre> To bridge thread-locals into the chain, use Reactor's context propagation
	 * support.
	 *
	 * @see SyncSpec#transportContextProvider(Supplier)
	 */
	class AsyncSpec {

		// 与 SyncSpec 一样是可变构建器：setter 返回当前 this，build() 才创建客户端，初始化由后续订阅触发。
		// 主要区别在回调签名：Consumer<T> 变为 Function<T, Mono<Void>>，有返回值的 handler 变为 Mono<Result>。
		// SDK 会组合并订阅回调返回的 Mono，回调应返回这条处理链；无需在回调内自行 subscribe，也不能返回 null。
		// 异步回调没有统一切换线程的包装；需要阻塞处理时由应用安排调度器。监听器追加后也不保证串行执行。
		// 此处没有 contextProvider；调用方通过 contextWrite 写入 McpTransportContext.KEY，按订阅传播上下文。

		/**
		 * async(transport) 必传的消息收发通道，决定 stdio/HTTP 等连接方式及可协商的协议版本列表。 地址、认证和连接参数在
		 * transport 上配置；build() 保存该对象，客户端关闭时也会关闭它。
		 */
		private final McpClientTransport transport;

		/**
		 * 单次出站 JSON-RPC 请求等待响应的上限，默认 20 秒，由 McpClientSession 实施。 覆盖 initialize、tools/call
		 * 等请求，不覆盖无需响应的通知；超时以 Mono 错误信号交给调用方。 不代表远端执行被取消，也不替代 transport 的连接超时。
		 */
		private Duration requestTimeout = Duration.ofSeconds(20); // Default timeout

		/**
		 * 等待握手和初始化后置工作的上限，默认 20 秒，由 LifecycleInitializer 实施。
		 * 多个首次调用可等待同一初始化过程；完成后才进入各自业务请求，业务等待另受 requestTimeout 约束。
		 */
		private Duration initializationTimeout = Duration.ofSeconds(20);

		/**
		 * initialize 中的客户端能力声明：roots、sampling、form/url elicitation 等。 默认 null，build() 按非空
		 * roots 和配置的 handler 推导；显式值会整体采用，不自动补充缺失能力。 声明 sampling/elicitation 后须提供匹配
		 * handler；对端是否支持 tools 等由服务端能力决定。
		 */
		private ClientCapabilities capabilities;

		/**
		 * 初始化时发给服务端的客户端实现信息。默认名称和版本见下面的 builder，版本字符串 0.15.0 不是 MCP 协议版本，不参与
		 * protocolVersions 的协商，也不作为可信用户身份。
		 */
		private Implementation clientInfo = Implementation.builder("Java SDK MCP Client", "0.15.0").build();

		/**
		 * 回答服务端 roots/list 的根 URI 定义，默认空；roots(...) 按 URI 合并，同 URI 后值覆盖前值。
		 * 不会主动读取这些目录。capabilities 未指定且 roots 非空时推导 roots(listChanged=false)。 如需运行期
		 * addRoot/removeRoot 自动通知对端，应显式配置 roots(true) 能力。
		 */
		private final Map<String, Root> roots = new HashMap<>();

		/**
		 * 工具目录变更回调，默认空。SDK 收到 tools/list_changed 后重新 listTools，再传入完整定义列表。 回调返回 Mono<Void>
		 * 表示本次刷新处理何时结束；配置回调不会立即查询工具。
		 */
		private final List<Function<List<McpSchema.Tool>, Mono<Void>>> toolsChangeConsumers = new ArrayList<>();

		/**
		 * 资源目录变更回调，默认空。收到 resources/list_changed 后重新 listResources，传入资源元数据列表。 返回的 Mono
		 * 表示处理完成；资源内容变化由 resourcesUpdateConsumers 单独处理。
		 */
		private final List<Function<List<McpSchema.Resource>, Mono<Void>>> resourcesChangeConsumers = new ArrayList<>();

		/**
		 * 资源内容变更回调，默认空。收到 resources/updated 后按 URI 读取资源，传入返回的 contents 列表。
		 * 本配置只登记监听器；需要订阅服务端资源更新时，另调用并订阅客户端 subscribeResource。
		 */
		private final List<Function<List<McpSchema.ResourceContents>, Mono<Void>>> resourcesUpdateConsumers = new ArrayList<>();

		/**
		 * 提示词目录变更回调，默认空。收到 prompts/list_changed 后重新 listPrompts，传入定义列表。 回调接收的不是 getPrompt
		 * 展开后的消息；返回的 Mono 表示消费这份列表的过程。
		 */
		private final List<Function<List<McpSchema.Prompt>, Mono<Void>>> promptsChangeConsumers = new ArrayList<>();

		/**
		 * 服务端 notifications/message 日志回调，默认空，参数含日志级别、logger 名称和 data。 返回 Mono
		 * 表示日志处理过程；注册它不设置 SDK 的本地日志级别或服务端日志阈值。
		 */
		private final List<Function<McpSchema.LoggingMessageNotification, Mono<Void>>> loggingConsumers = new ArrayList<>();

		/**
		 * notifications/progress 回调，默认空，接收进度数值及 progressToken 等。 调用方自行在请求 _meta 中放入
		 * progressToken，再用通知中的同一 token 关联任务； 回调不是最终 RPC 响应，SDK 不会因为进度通知到达就完成原请求。
		 */
		private final List<Function<McpSchema.ProgressNotification, Mono<Void>>> progressConsumers = new ArrayList<>();

		/**
		 * URL 引导完成通知回调，默认空，按通知中的 elicitationId 与先前 URL 交互关联。 这里消费
		 * notifications/elicitation/complete；立即回答引导请求使用 urlElicitationHandler。
		 */
		private final List<Function<McpSchema.ElicitationCompleteNotification, Mono<Void>>> elicitationCompleteConsumers = new ArrayList<>();

		/**
		 * 处理服务端 sampling/createMessage 的异步函数，默认 null；应用调用自己的模型并返回结果 Mono。 SDK
		 * 把该结果作为反向请求的响应发回服务端，不会自动调用某个模型供应商。 未显式设置 capabilities 时，配置此 handler 会推导 sampling
		 * 能力。
		 */
		private Function<CreateMessageRequest, Mono<CreateMessageResult>> samplingHandler;

		/**
		 * form 模式 elicitation/create 的异步函数，默认 null，通过 elicitation(...) 配置。 根据
		 * message/requestedSchema 获取用户输入，再通过 Mono 返回 action 和 content；SDK 不生成 UI。 未显式设置
		 * capabilities 时推导 form 能力；返回值还可能经过 applyElicitationDefaults 处理。
		 */
		private Function<ElicitFormRequest, Mono<ElicitResult>> formElicitationHandler;

		/**
		 * URL 模式 elicitation/create 的异步函数，默认 null，通过 urlElicitation(...) 配置。 应用负责 URL
		 * 展示/跳转及用户交互，以 Mono 返回决定；SDK 不自动打开浏览器。 未显式设置 capabilities 时推导 URL 能力，外部流程完成通知由
		 * elicitationCompleteConsumers 处理。
		 */
		private Function<ElicitUrlRequest, Mono<ElicitResult>> urlElicitationHandler;

		/**
		 * 工具结构化输出的校验实现，默认 null，build() 时从 McpJsonDefaults 加载默认 provider。 配合
		 * enableCallToolSchemaCaching 校验 structuredContent；验证失败使 callTool 以错误信号结束。 这是
		 * JSON Schema 校验器，消息的 JSON 编解码仍由 transport 使用自己的 mapper 完成。
		 */
		private JsonSchemaValidator jsonSchemaValidator;

		/**
		 * 工具输出 schema 缓存与结果校验开关，默认 false。开启且服务端有 tools 能力时，初始化查询第一页； 后续 listTools
		 * 也会把本次查到的非空 outputSchema 按工具名写入缓存，不缓存工具执行结果。 当前 callTool
		 * 不会在缓存缺失时自动补查；缺失时告警并返回，isError=true 时跳过输出校验。
		 */
		private boolean enableCallToolSchemaCaching = false; // Default to false

		/**
		 * 表单结果默认值补齐开关，默认 false。form handler 的 Mono 返回 ACCEPT 且 content 非 null 时， SDK
		 * 复制内容并为不存在的顶层字段补 requestedSchema.properties 中的 default。 不覆盖已有键，不递归，不处理
		 * URL/拒绝/取消结果；此开关不序列化进 initialize 能力声明。
		 */
		private boolean applyElicitationDefaults = false; // Default to false

		private AsyncSpec(McpClientTransport transport) {
			Assert.notNull(transport, "Transport must not be null");
			this.transport = transport;
		}

		/**
		 * Sets the duration to wait for server responses before timing out requests. This
		 * timeout applies to all requests made through the client, including tool calls,
		 * resource access, and prompt operations.
		 * @param requestTimeout The duration to wait before timing out requests. Must not
		 * be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if requestTimeout is null
		 */
		public AsyncSpec requestTimeout(Duration requestTimeout) {
			Assert.notNull(requestTimeout, "Request timeout must not be null");
			this.requestTimeout = requestTimeout;
			return this;
		}

		/**
		 * @param initializationTimeout The duration to wait for the initialization
		 * lifecycle step to complete.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if initializationTimeout is null
		 */
		public AsyncSpec initializationTimeout(Duration initializationTimeout) {
			Assert.notNull(initializationTimeout, "Initialization timeout must not be null");
			this.initializationTimeout = initializationTimeout;
			return this;
		}

		/**
		 * Sets the client capabilities that will be advertised to the server during
		 * connection initialization. Capabilities describe client-side features such as
		 * roots, sampling, and elicitation; server capabilities are negotiated
		 * separately.
		 * @param capabilities The client capabilities configuration. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if capabilities is null
		 */
		public AsyncSpec capabilities(ClientCapabilities capabilities) {
			Assert.notNull(capabilities, "Capabilities must not be null");
			this.capabilities = capabilities;
			return this;
		}

		/**
		 * Sets the client implementation information that will be shared with the server
		 * during connection initialization. This helps with version compatibility and
		 * debugging.
		 * @param clientInfo The client implementation details including name and version.
		 * Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if clientInfo is null
		 */
		public AsyncSpec clientInfo(Implementation clientInfo) {
			Assert.notNull(clientInfo, "Client info must not be null");
			this.clientInfo = clientInfo;
			return this;
		}

		/**
		 * Sets the root URIs that this client can access. Roots define the base URIs for
		 * resources that the client can request from the server. For example, a root
		 * might be "file://workspace" for accessing workspace files.
		 * <p>
		 * Roots are deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * answering {@code roots/list} for servers that still use roots.
		 * @param roots A list of root definitions. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if roots is null
		 */
		public AsyncSpec roots(List<Root> roots) {
			Assert.notNull(roots, "Roots must not be null");
			for (Root root : roots) {
				this.roots.put(root.uri(), root);
			}
			return this;
		}

		/**
		 * Sets the root URIs that this client can access, using a varargs parameter for
		 * convenience. This is an alternative to {@link #roots(List)}.
		 * <p>
		 * Roots are deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * answering {@code roots/list} for servers that still use roots.
		 * @param roots An array of root definitions. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if roots is null
		 * @see #roots(List)
		 */
		public AsyncSpec roots(Root... roots) {
			Assert.notNull(roots, "Roots must not be null");
			for (Root root : roots) {
				this.roots.put(root.uri(), root);
			}
			return this;
		}

		/**
		 * Sets a handler for sampling/createMessage requests received from the server.
		 * The application invokes its model and returns the generated message result.
		 * <p>
		 * Sampling is deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * handling {@code sampling/createMessage} for servers that still use sampling.
		 * @param samplingHandler A function that processes message requests and returns
		 * results. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if samplingHandler is null
		 */
		public AsyncSpec sampling(Function<CreateMessageRequest, Mono<CreateMessageResult>> samplingHandler) {
			Assert.notNull(samplingHandler, "Sampling handler must not be null");
			this.samplingHandler = samplingHandler;
			return this;
		}

		/**
		 * Sets a handler for form-mode elicitation requests received from the server. The
		 * application collects user input and returns an elicitation result.
		 * @param elicitationHandler A function that processes elicitation requests and
		 * returns results. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationHandler is null
		 */
		public AsyncSpec elicitation(Function<ElicitFormRequest, Mono<ElicitResult>> elicitationHandler) {
			Assert.notNull(elicitationHandler, "Elicitation handler must not be null");
			this.formElicitationHandler = elicitationHandler;
			return this;
		}

		/**
		 * Sets a handler for URL-mode elicitation requests received from the server. The
		 * application guides the user to the supplied URL and returns their decision.
		 * @param elicitationHandler A function that processes elicitation requests and
		 * returns results. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationHandler is null
		 */
		public AsyncSpec urlElicitation(Function<ElicitUrlRequest, Mono<ElicitResult>> elicitationHandler) {
			Assert.notNull(elicitationHandler, "Elicitation handler must not be null");
			this.urlElicitationHandler = elicitationHandler;
			return this;
		}

		/**
		 * Adds a consumer to be notified when the available tools change. This allows the
		 * client to react to changes in the server's tool capabilities, such as tools
		 * being added or removed.
		 * @param toolsChangeConsumer A consumer that receives the updated list of
		 * available tools. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if toolsChangeConsumer is null
		 */
		public AsyncSpec toolsChangeConsumer(Function<List<McpSchema.Tool>, Mono<Void>> toolsChangeConsumer) {
			Assert.notNull(toolsChangeConsumer, "Tools change consumer must not be null");
			this.toolsChangeConsumers.add(toolsChangeConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when the available resources change. This allows
		 * the client to react to changes in the server's resource availability, such as
		 * files being added or removed.
		 * @param resourcesChangeConsumer A consumer that receives the updated list of
		 * available resources. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if resourcesChangeConsumer is null
		 */
		public AsyncSpec resourcesChangeConsumer(
				Function<List<McpSchema.Resource>, Mono<Void>> resourcesChangeConsumer) {
			Assert.notNull(resourcesChangeConsumer, "Resources change consumer must not be null");
			this.resourcesChangeConsumers.add(resourcesChangeConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when a specific resource is updated. This allows
		 * the client to react to changes in individual resources, such as updates to
		 * their content or metadata.
		 * @param resourcesUpdateConsumer A consumer function that processes the updated
		 * resource and returns a Mono indicating the completion of the processing. Must
		 * not be null.
		 * @return This builder instance for method chaining.
		 * @throws IllegalArgumentException If the resourcesUpdateConsumer is null.
		 */
		public AsyncSpec resourcesUpdateConsumer(
				Function<List<McpSchema.ResourceContents>, Mono<Void>> resourcesUpdateConsumer) {
			Assert.notNull(resourcesUpdateConsumer, "Resources update consumer must not be null");
			this.resourcesUpdateConsumers.add(resourcesUpdateConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when the available prompts change. This allows
		 * the client to react to changes in the server's prompt templates, such as new
		 * templates being added or existing ones being modified.
		 * @param promptsChangeConsumer A consumer that receives the updated list of
		 * available prompts. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if promptsChangeConsumer is null
		 */
		public AsyncSpec promptsChangeConsumer(Function<List<McpSchema.Prompt>, Mono<Void>> promptsChangeConsumer) {
			Assert.notNull(promptsChangeConsumer, "Prompts change consumer must not be null");
			this.promptsChangeConsumers.add(promptsChangeConsumer);
			return this;
		}

		/**
		 * Adds a consumer to be notified when logging messages are received from the
		 * server. This allows the client to react to log messages, such as warnings or
		 * errors, that are sent by the server.
		 * <p>
		 * Logging is deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * handling log messages from servers that still send them.
		 * @param loggingConsumer A consumer that receives logging messages. Must not be
		 * null.
		 * @return This builder instance for method chaining
		 */
		public AsyncSpec loggingConsumer(Function<McpSchema.LoggingMessageNotification, Mono<Void>> loggingConsumer) {
			Assert.notNull(loggingConsumer, "Logging consumer must not be null");
			this.loggingConsumers.add(loggingConsumer);
			return this;
		}

		/**
		 * Adds multiple consumers to be notified when logging messages are received from
		 * the server. This allows the client to react to log messages, such as warnings
		 * or errors, that are sent by the server.
		 * <p>
		 * Logging is deprecated as of MCP protocol version 2026-07-28 (SEP-2577). This
		 * method is not deprecated because the specification requires clients to keep
		 * handling log messages from servers that still send them.
		 * @param loggingConsumers A list of consumers that receive logging messages. Must
		 * not be null.
		 * @return This builder instance for method chaining
		 */
		public AsyncSpec loggingConsumers(
				List<Function<McpSchema.LoggingMessageNotification, Mono<Void>>> loggingConsumers) {
			Assert.notNull(loggingConsumers, "Logging consumers must not be null");
			this.loggingConsumers.addAll(loggingConsumers);
			return this;
		}

		/**
		 * Adds a consumer to be notified of progress notifications from the server. This
		 * allows the client to track long-running operations and provide feedback to
		 * users.
		 * @param progressConsumer A consumer that receives progress notifications. Must
		 * not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if progressConsumer is null
		 */
		public AsyncSpec progressConsumer(Function<McpSchema.ProgressNotification, Mono<Void>> progressConsumer) {
			Assert.notNull(progressConsumer, "Progress consumer must not be null");
			this.progressConsumers.add(progressConsumer);
			return this;
		}

		/**
		 * Adds a multiple consumers to be notified of progress notifications from the
		 * server. This allows the client to track long-running operations and provide
		 * feedback to users.
		 * @param progressConsumers A list of consumers that receives progress
		 * notifications. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if progressConsumer is null
		 */
		public AsyncSpec progressConsumers(
				List<Function<McpSchema.ProgressNotification, Mono<Void>>> progressConsumers) {
			Assert.notNull(progressConsumers, "Progress consumers must not be null");
			this.progressConsumers.addAll(progressConsumers);
			return this;
		}

		/**
		 * Adds a consumer to be notified by the server when an URL elicitation is
		 * complete.
		 * @param elicitationCompleteConsumer A consumer that receives elicitation
		 * complete notifications. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationCompleteConsumer is null
		 */
		public AsyncSpec elicitationCompleteConsumer(
				Function<McpSchema.ElicitationCompleteNotification, Mono<Void>> elicitationCompleteConsumer) {
			Assert.notNull(elicitationCompleteConsumer, "Elicitation complete consumer must not be null");
			this.elicitationCompleteConsumers.add(elicitationCompleteConsumer);
			return this;
		}

		/**
		 * Adds multiple consumers to be notified by the server when an URL elicitation is
		 * complete.
		 * @param elicitationCompleteConsumers A list of consumers that receives
		 * elicitation complete notifications. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if elicitationCompleteConsumers is null
		 */
		public AsyncSpec elicitationCompleteConsumers(
				List<Function<McpSchema.ElicitationCompleteNotification, Mono<Void>>> elicitationCompleteConsumers) {
			Assert.notNull(elicitationCompleteConsumers, "Elicitation complete consumers must not be null");
			this.elicitationCompleteConsumers.addAll(elicitationCompleteConsumers);
			return this;
		}

		/**
		 * Sets the JSON schema validator to use for validating tool responses against
		 * output schemas.
		 * @param jsonSchemaValidator The validator to use. Must not be null.
		 * @return This builder instance for method chaining
		 * @throws IllegalArgumentException if jsonSchemaValidator is null
		 */
		public AsyncSpec jsonSchemaValidator(JsonSchemaValidator jsonSchemaValidator) {
			Assert.notNull(jsonSchemaValidator, "JsonSchemaValidator must not be null");
			this.jsonSchemaValidator = jsonSchemaValidator;
			return this;
		}

		/**
		 * Enables output-schema caching during initialization and listTools requests, and
		 * validation of successful callTool results against cached schemas.
		 * Initialization fetches the first page; callTool does not fetch missing schemas.
		 * @param enableCallToolSchemaCaching true to enable, false to disable
		 * @return This builder instance for method chaining
		 */
		public AsyncSpec enableCallToolSchemaCaching(boolean enableCallToolSchemaCaching) {
			this.enableCallToolSchemaCaching = enableCallToolSchemaCaching;
			return this;
		}

		/**
		 * Enables SDK-side merging of elicitation schema defaults into an accepted
		 * {@link ElicitResult}'s {@code content} for fields the elicitation handler left
		 * unset. This is a client-local behavior and is NOT serialized as part of the MCP
		 * capability handshake.
		 * @param applyElicitationDefaults true to enable, false to disable
		 * @return This builder instance for method chaining
		 */
		public AsyncSpec applyElicitationDefaults(boolean applyElicitationDefaults) {
			this.applyElicitationDefaults = applyElicitationDefaults;
			return this;
		}

		/**
		 * Create an instance of {@link McpAsyncClient} with the provided configurations
		 * or sensible defaults.
		 * @return a new instance of {@link McpAsyncClient}.
		 */
		public McpAsyncClient build() {
			// build 组装特性和初始化器；会话由初始化器首次被订阅时按需创建，
			// 因而构建客户端不等于已经完成连接或 initialize 协商。
			var jsonSchemaValidator = (this.jsonSchemaValidator != null) ? this.jsonSchemaValidator
					: McpJsonDefaults.getSchemaValidator();
			return new McpAsyncClient(this.transport, this.requestTimeout, this.initializationTimeout,
					jsonSchemaValidator,
					new McpClientFeatures.Async(this.clientInfo, this.capabilities, this.roots,
							this.toolsChangeConsumers, this.resourcesChangeConsumers, this.resourcesUpdateConsumers,
							this.promptsChangeConsumers, this.loggingConsumers, this.progressConsumers,
							this.elicitationCompleteConsumers, this.samplingHandler, this.formElicitationHandler,
							this.urlElicitationHandler, this.enableCallToolSchemaCaching,
							this.applyElicitationDefaults));
		}

	}

}
