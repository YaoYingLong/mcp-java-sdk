/*
 * Copyright 2024-2025 the original author or authors.
 */

package io.modelcontextprotocol.spec;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.schema.JsonSchemaValidator;
import io.modelcontextprotocol.server.McpAsyncServerExchange;
import io.modelcontextprotocol.server.McpInitRequestHandler;
import io.modelcontextprotocol.server.McpNotificationHandler;
import io.modelcontextprotocol.server.McpRequestHandler;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

/**
 * Represents a Model Context Protocol (MCP) session on the server side. It manages
 * bidirectional JSON-RPC communication with the client.
 */
/*
 * stdio/旧 SSE 的一对一逻辑会话：一个 transport 同时承担请求、响应和通知的双向通道。 incoming request 按 method 找服务器提供的
 * handler；incoming response 按 id 完成服务端反向请求。 initialize 保存客户端信息并返回协商结果，initialized 通知才发布
 * exchangeSink，普通 handler 等待此门闩。 state 字段记录握手阶段，但当前实现仍保留多处 TODO，不能把它理解为覆盖全部非法顺序的校验器。
 */
public class McpServerSession implements McpLoggableSession {

	private static final Logger logger = LoggerFactory.getLogger(McpServerSession.class);

	private final ConcurrentHashMap<Object, MonoSink<McpSchema.JSONRPCResponse>> pendingResponses = new ConcurrentHashMap<>();

	private final String id;

	/** Duration to wait for request responses before timing out */
	private final Duration requestTimeout;

	private final AtomicLong requestCounter = new AtomicLong(0);

	private final McpInitRequestHandler initRequestHandler;

	private final Map<String, McpRequestHandler<?>> requestHandlers;

	private final Map<String, McpNotificationHandler> notificationHandlers;

	private final McpServerTransport transport;

	private final Sinks.One<McpAsyncServerExchange> exchangeSink = Sinks.one();

	private final AtomicReference<McpSchema.ClientCapabilities> clientCapabilities = new AtomicReference<>();

	private final AtomicReference<McpSchema.Implementation> clientInfo = new AtomicReference<>();

	private static final int STATE_UNINITIALIZED = 0;

	private static final int STATE_INITIALIZING = 1;

	private static final int STATE_INITIALIZED = 2;

	private final AtomicInteger state = new AtomicInteger(STATE_UNINITIALIZED);

	private volatile McpSchema.LoggingLevel minLoggingLevel = McpSchema.LoggingLevel.INFO;

	private final Supplier<Mono<Void>> onClose;

	private final JsonSchemaValidator jsonSchemaValidator;

	/**
	 * Creates a new server session with the given parameters and the transport to use.
	 * @param id session id
	 * @param requestTimeout duration to wait for request responses before timing out
	 * @param transport the transport to use
	 * @param initHandler called when a
	 * {@link io.modelcontextprotocol.spec.McpSchema.InitializeRequest} is received by the
	 * server
	 * @param requestHandlers map of request handlers to use
	 * @param notificationHandlers map of notification handlers to use
	 * @param onClose supplier of a reactive callback invoked when the session is closed
	 * @param jsonSchemaValidator optional validator threaded to exchanges for elicitation
	 * schema validation
	 */
	public McpServerSession(String id, Duration requestTimeout, McpServerTransport transport,
			McpInitRequestHandler initHandler, Map<String, McpRequestHandler<?>> requestHandlers,
			Map<String, McpNotificationHandler> notificationHandlers, Supplier<Mono<Void>> onClose,
			JsonSchemaValidator jsonSchemaValidator) {
		this.id = id;
		this.requestTimeout = requestTimeout;
		this.transport = transport;
		this.initRequestHandler = initHandler;
		this.requestHandlers = requestHandlers;
		this.notificationHandlers = notificationHandlers;
		this.onClose = onClose;
		this.jsonSchemaValidator = jsonSchemaValidator;
	}

	/**
	 * Creates a new server session with the given parameters and the transport to use.
	 * @param id session id
	 * @param requestTimeout duration to wait for request responses before timing out
	 * @param transport the transport to use
	 * @param initHandler called when a
	 * {@link io.modelcontextprotocol.spec.McpSchema.InitializeRequest} is received by the
	 * server
	 * @param requestHandlers map of request handlers to use
	 * @param notificationHandlers map of notification handlers to use
	 * @param onClose supplier of a reactive callback invoked when the session is closed
	 */
	public McpServerSession(String id, Duration requestTimeout, McpServerTransport transport,
			McpInitRequestHandler initHandler, Map<String, McpRequestHandler<?>> requestHandlers,
			Map<String, McpNotificationHandler> notificationHandlers, Supplier<Mono<Void>> onClose) {
		this(id, requestTimeout, transport, initHandler, requestHandlers, notificationHandlers, onClose, null);
	}

	/**
	 * Creates a new server session with the given parameters and the transport to use.
	 * @param id session id
	 * @param requestTimeout duration to wait for request responses before timing out
	 * @param transport the transport to use
	 * @param initHandler called when a
	 * {@link io.modelcontextprotocol.spec.McpSchema.InitializeRequest} is received by the
	 * server
	 * @param requestHandlers map of request handlers to use
	 * @param notificationHandlers map of notification handlers to use
	 */
	public McpServerSession(String id, Duration requestTimeout, McpServerTransport transport,
			McpInitRequestHandler initHandler, Map<String, McpRequestHandler<?>> requestHandlers,
			Map<String, McpNotificationHandler> notificationHandlers) {
		this(id, requestTimeout, transport, initHandler, requestHandlers, notificationHandlers, Mono::empty);
	}

	/**
	 * Retrieve the session id.
	 * @return session id
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * Called upon successful initialization sequence between the client and the server
	 * with the client capabilities and information.
	 *
	 * <a href=
	 * "https://github.com/modelcontextprotocol/specification/blob/main/docs/specification/basic/lifecycle.md#initialization">Initialization
	 * Spec</a>
	 * @param clientCapabilities the capabilities the connected client provides
	 * @param clientInfo the information about the connected client
	 */
	public void init(McpSchema.ClientCapabilities clientCapabilities, McpSchema.Implementation clientInfo) {
		this.clientCapabilities.lazySet(clientCapabilities);
		this.clientInfo.lazySet(clientInfo);
	}

	private String generateRequestId() {
		return this.id + "-" + this.requestCounter.getAndIncrement();
	}

	@Override
	public void setMinLoggingLevel(McpSchema.LoggingLevel minLoggingLevel) {
		Assert.notNull(minLoggingLevel, "minLoggingLevel must not be null");
		this.minLoggingLevel = minLoggingLevel;
	}

	@Override
	public boolean isNotificationForLevelAllowed(McpSchema.LoggingLevel loggingLevel) {
		return loggingLevel.level() >= this.minLoggingLevel.level();
	}

	@Override
	// 服务端反向请求：先登记等待槽再发送，以免客户端响应过快而无法匹配；send 完成只说明发送阶段完成。
	// 真正结果由 handle 收到同 id 的 JSONRPCResponse 后写回 sink，timeout 限制等待时长。
	// 注意本 legacy 实现没有 Streamable 版本的 doFinally 清理，勿假设两种实现生命周期完全一致。
	public <T> Mono<T> sendRequest(String method, Object requestParams, TypeRef<T> typeRef) {
		String requestId = this.generateRequestId();

		return Mono.<McpSchema.JSONRPCResponse>create(sink -> {
			this.pendingResponses.put(requestId, sink);
			McpSchema.JSONRPCRequest jsonrpcRequest = new McpSchema.JSONRPCRequest(method, requestId, requestParams);
			this.transport.sendMessage(jsonrpcRequest).subscribe(v -> {
			}, error -> {
				this.pendingResponses.remove(requestId);
				sink.error(error);
			});
		}).timeout(requestTimeout).handle((jsonRpcResponse, sink) -> {
			if (jsonRpcResponse.error() != null) {
				sink.error(new McpError(jsonRpcResponse.error()));
			}
			else {
				if (typeRef.getType().equals(Void.class)) {
					sink.complete();
				}
				else {
					sink.next(this.transport.unmarshalFrom(jsonRpcResponse.result(), typeRef));
				}
			}
		});
	}

	@Override
	public Mono<Void> sendNotification(String method, Object params) {
		McpSchema.JSONRPCNotification jsonrpcNotification = new McpSchema.JSONRPCNotification(method, params);
		return this.transport.sendMessage(jsonrpcNotification);
	}

	/**
	 * Called by the {@link McpServerTransportProvider} once the session is determined.
	 * The purpose of this method is to dispatch the message to an appropriate handler as
	 * specified by the MCP server implementation
	 * ({@link io.modelcontextprotocol.server.McpAsyncServer} or
	 * {@link io.modelcontextprotocol.server.McpSyncServer}) via
	 * {@link McpServerSession.Factory} that the server creates.
	 * @param message the incoming JSON-RPC message
	 * @return a Mono that completes when the message is processed
	 */
	// transport 已完成 JSON 解码，此层不再处理 HTTP 状态码或 SSE 帧格式。
	// 请求处理异常可转为带原 request.id 的响应；通知没有 id，不生成 JSON-RPC 响应。
	// 从 Reactor Context 读取本次元数据，避免把后续 POST 的身份信息误用为初始化请求的上下文。
	public Mono<Void> handle(McpSchema.JSONRPCMessage message) {
		return Mono.deferContextual(ctx -> {
			McpTransportContext transportContext = ctx.getOrDefault(McpTransportContext.KEY, McpTransportContext.EMPTY);

			// TODO handle errors for communication to without initialization happening
			// first
			if (message instanceof McpSchema.JSONRPCResponse response) {
				logger.debug("Received response: {}", response);
				if (response.id() != null) {
					var sink = pendingResponses.remove(response.id());
					if (sink == null) {
						logger.warn("Unexpected response for unknown id {}", response.id());
					}
					else {
						sink.success(response);
					}
				}
				else {
					logger.error("Discarded MCP request response without session id. "
							+ "This is an indication of a bug in the request sender code that can lead to memory "
							+ "leaks as pending requests will never be completed.");
				}
				return Mono.empty();
			}
			else if (message instanceof McpSchema.JSONRPCRequest request) {
				logger.debug("Received request: {}", request);
				return handleIncomingRequest(request, transportContext).onErrorResume(error -> {
					McpSchema.JSONRPCResponse.JSONRPCError jsonRpcError = toJsonRpcError(request, error);
					var errorResponse = McpSchema.JSONRPCResponse.error(request.id(), jsonRpcError);
					// TODO: Should the error go to SSE or back as POST return?
					return this.transport.sendMessage(errorResponse).then(Mono.empty());
				}).flatMap(this.transport::sendMessage);
			}
			else if (message instanceof McpSchema.JSONRPCNotification notification) {
				// TODO handle errors for communication to without initialization
				// happening first
				logger.debug("Received notification: {}", notification);
				// TODO: in case of error, should the POST request be signalled?
				return handleIncomingNotification(notification, transportContext).doOnError(
						error -> logger.warn("Error handling notification {}: {}", notification, error.getMessage()));
			}
			else {
				logger.warn("Received unknown message type: {}", message);
				return Mono.empty();
			}
		});
	}

	/**
	 * Handles an incoming JSON-RPC request by routing it to the appropriate handler.
	 * @param request The incoming JSON-RPC request
	 * @param transportContext
	 * @return A Mono containing the JSON-RPC response
	 */
	private Mono<McpSchema.JSONRPCResponse> handleIncomingRequest(McpSchema.JSONRPCRequest request,
			McpTransportContext transportContext) {
		return Mono.defer(() -> {
			Mono<?> resultMono;
			if (McpSchema.METHOD_INITIALIZE.equals(request.method())) {
				// TODO handle situation where already initialized!
				McpSchema.InitializeRequest initializeRequest = transport.unmarshalFrom(request.params(),
						new TypeRef<McpSchema.InitializeRequest>() {
						});

				this.state.lazySet(STATE_INITIALIZING);
				this.init(initializeRequest.capabilities(), initializeRequest.clientInfo());
				resultMono = this.initRequestHandler.handle(initializeRequest);
			}
			else {
				// TODO handle errors for communication to this session without
				// initialization happening first
				var handler = this.requestHandlers.get(request.method());
				if (handler == null) {
					MethodNotFoundError error = getMethodNotFoundError(request.method());
					return Mono
						.just(McpSchema.JSONRPCResponse.error(request.id(), new McpSchema.JSONRPCResponse.JSONRPCError(
								McpSchema.ErrorCodes.METHOD_NOT_FOUND, error.message(), error.data())));
				}

				resultMono = this.exchangeSink.asMono()
					.flatMap(exchange -> handler.handle(copyExchange(exchange, transportContext), request.params()));
			}
			return resultMono.map(result -> McpSchema.JSONRPCResponse.result(request.id(), result))
				.onErrorResume(error -> Mono
					.just(McpSchema.JSONRPCResponse.error(request.id(), toJsonRpcError(request, error))));
		});
	}

	/**
	 * Converts a request handler failure into the JSON-RPC error sent to the client.
	 * Errors other than {@link McpError} are logged, and only their message is sent:
	 * their cause chain may contain internal details that must not reach the client.
	 * @param request the request that failed
	 * @param error the failure
	 * @return the JSON-RPC error to send to the client
	 */
	private McpSchema.JSONRPCResponse.JSONRPCError toJsonRpcError(McpSchema.JSONRPCRequest request, Throwable error) {
		if (error instanceof McpError mcpError && mcpError.getJsonRpcError() != null) {
			return mcpError.getJsonRpcError();
		}
		logger.warn("Error handling request {} [{}]", request.id(), request.method(), error);
		return new McpSchema.JSONRPCResponse.JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR,
				Objects.requireNonNullElse(error.getMessage(), "Internal error"), null);
	}

	/**
	 * Handles an incoming JSON-RPC notification by routing it to the appropriate handler.
	 * @param notification The incoming JSON-RPC notification
	 * @param transportContext
	 * @return A Mono that completes when the notification is processed
	 */
	private Mono<Void> handleIncomingNotification(McpSchema.JSONRPCNotification notification,
			McpTransportContext transportContext) {
		return Mono.defer(() -> {
			if (McpSchema.METHOD_NOTIFICATION_INITIALIZED.equals(notification.method())) {
				this.state.lazySet(STATE_INITIALIZED);
				// FIXME: The session ID passed here is not the same as the one in the
				// legacy SSE transport.
				exchangeSink.tryEmitValue(new McpAsyncServerExchange(this.id, this, clientCapabilities.get(),
						clientInfo.get(), transportContext, this.jsonSchemaValidator));
			}

			var handler = notificationHandlers.get(notification.method());
			if (handler == null) {
				logger.warn("No handler registered for notification method: {}", notification);
				return Mono.empty();
			}
			return this.exchangeSink.asMono()
				.flatMap(exchange -> handler.handle(copyExchange(exchange, transportContext), notification.params()));
		});
	}

	/**
	 * This legacy implementation assumes an exchange is established upon the
	 * initialization phase see: exchangeSink.tryEmitValue(...), which creates a cached
	 * immutable exchange. Here, we create a new exchange and copy over everything from
	 * that cached exchange, and use it for a single HTTP request, with the transport
	 * context passed in.
	 */
	// 复用协商得到的客户端信息与同一会话，但替换请求级上下文；并非重做握手，也不复制 transport。
	private McpAsyncServerExchange copyExchange(McpAsyncServerExchange exchange, McpTransportContext transportContext) {
		return new McpAsyncServerExchange(exchange.sessionId(), this, exchange.getClientCapabilities(),
				exchange.getClientInfo(), transportContext, this.jsonSchemaValidator);
	}

	record MethodNotFoundError(String method, String message, Object data) {
	}

	private MethodNotFoundError getMethodNotFoundError(String method) {
		return new MethodNotFoundError(method, "Method not found: " + method, null);
	}

	@Override
	// onClose 由服务器注入（清理资源订阅等），错误在这里吞掉以继续关闭底层 transport。
	// 下方 TODO 表明 pendingResponses 尚未在关闭时统一失败，关闭完成不等价于所有业务等待均已解锁。
	public Mono<Void> closeGracefully() {
		// TODO: clear pendingResponses and emit errors?
		return this.onClose.get().onErrorComplete().then(this.transport.closeGracefully());
	}

	@Override
	public void close() {
		// TODO: clear pendingResponses and emit errors?
		this.onClose.get().onErrorComplete().subscribe();
		this.transport.close();
	}

	/**
	 * Notification handler for the initialization notification from the client.
	 */
	public interface InitNotificationHandler {

		/**
		 * Specifies an action to take upon successful initialization.
		 * @return a Mono that will complete when the initialization is acted upon.
		 */
		Mono<Void> handle();

	}

	/**
	 * Factory for creating server sessions which delegate to a provided 1:1 transport
	 * with a connected client.
	 */
	@FunctionalInterface
	public interface Factory {

		/**
		 * Creates a new 1:1 representation of the client-server interaction.
		 * @param sessionTransport the transport to use for communication with the client.
		 * @return a new server session.
		 */
		McpServerSession create(McpServerTransport sessionTransport);

	}

}
