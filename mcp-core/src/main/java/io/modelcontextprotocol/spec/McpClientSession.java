/*
 * Copyright 2024-2024 the original author or authors.
 */

package io.modelcontextprotocol.spec;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.util.Assert;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Default implementation of the MCP (Model Context Protocol) session that manages
 * bidirectional JSON-RPC communication between clients and servers. This implementation
 * follows the MCP specification for message exchange and transport handling.
 *
 * <p>
 * The session manages:
 * <ul>
 * <li>Request/response handling with unique message IDs</li>
 * <li>Notification processing</li>
 * <li>Message timeout management</li>
 * <li>Transport layer abstraction</li>
 * </ul>
 *
 * @author Christian Tzolov
 * @author Dariusz Jędrzejczyk
 * @author Yanming Zhou
 */
// 客户端 JSON-RPC 分发中心：收到 response 按 ID 唤醒发起方，收到 request 按 method
// 调用客户端处理器并回写 response，收到 notification 则执行无响应的处理器。
// 它不解释 tools 等业务语义，也不保存 HTTP 的 Mcp-Session-Id；两个“会话”属于不同层。
public class McpClientSession implements McpSession {

	private static final Logger logger = LoggerFactory.getLogger(McpClientSession.class);

	/** Duration to wait for request responses before timing out */
	private final Duration requestTimeout;

	/** Transport layer implementation for message exchange */
	private final McpClientTransport transport;

	/** Map of pending responses keyed by request ID */
	private final ConcurrentHashMap<Object, MonoSink<McpSchema.JSONRPCResponse>> pendingResponses = new ConcurrentHashMap<>();

	/** Map of request handlers keyed by method name */
	private final ConcurrentHashMap<String, RequestHandler<?>> requestHandlers = new ConcurrentHashMap<>();

	/** Map of notification handlers keyed by method name */
	private final ConcurrentHashMap<String, NotificationHandler> notificationHandlers = new ConcurrentHashMap<>();

	/** Session-specific prefix for request IDs */
	private final String sessionPrefix = UUID.randomUUID().toString().substring(0, 8);

	/** Atomic counter for generating unique request IDs */
	private final AtomicLong requestCounter = new AtomicLong(0);

	/**
	 * Functional interface for handling incoming JSON-RPC requests. Implementations
	 * should process the request parameters and return a response.
	 *
	 * @param <T> Response type
	 */
	@FunctionalInterface
	public interface RequestHandler<T> {

		/**
		 * Handles an incoming request with the given parameters.
		 * @param params The request parameters
		 * @return A Mono containing the response object
		 */
		Mono<T> handle(Object params);

	}

	/**
	 * Functional interface for handling incoming JSON-RPC notifications. Implementations
	 * should process the notification parameters without returning a response.
	 */
	@FunctionalInterface
	public interface NotificationHandler {

		/**
		 * Handles an incoming notification with the given parameters.
		 * @param params The notification parameters
		 * @return A Mono that completes when the notification is processed
		 */
		Mono<Void> handle(Object params);

	}

	/**
	 * Creates a new McpClientSession with the specified configuration and handlers.
	 * @param requestTimeout Duration to wait for responses
	 * @param transport Transport implementation for message exchange
	 * @param requestHandlers Map of method names to request handlers
	 * @param notificationHandlers Map of method names to notification handlers
	 * @param connectHook Hook that allows transforming the connection Publisher prior to
	 * subscribing
	 */
	public McpClientSession(Duration requestTimeout, McpClientTransport transport,
			Map<String, RequestHandler<?>> requestHandlers, Map<String, NotificationHandler> notificationHandlers,
			Function<? super Mono<Void>, ? extends Publisher<Void>> connectHook) {

		Assert.notNull(requestTimeout, "The requestTimeout can not be null");
		Assert.notNull(transport, "The transport can not be null");
		Assert.notNull(requestHandlers, "The requestHandlers can not be null");
		Assert.notNull(notificationHandlers, "The notificationHandlers can not be null");

		this.requestTimeout = requestTimeout;
		this.transport = transport;
		this.requestHandlers.putAll(requestHandlers);
		this.notificationHandlers.putAll(notificationHandlers);

		// 构造会话即订阅连接；connectHook 可在订阅前恢复初始化调用方的 Context。
		// 这里只记录连接错误，协议握手仍通过后续 initialize RPC 的结果判断成功与否。
		this.transport.connect(mono -> mono.doOnNext(this::handle)).transform(connectHook).subscribe(ignored -> {
		}, error -> logger.warn("Client failed during connect", error));
	}

	private void dismissPendingResponses() {
		this.pendingResponses.forEach((id, sink) -> {
			logger.info("Abruptly terminating exchange for request {}", id);
			sink.error(new RuntimeException("MCP session with server terminated"));
		});
		this.pendingResponses.clear();
	}

	private void handle(McpSchema.JSONRPCMessage message) {
		// 此入口由传输的入站流驱动。下面对反向 request/notification 单独 subscribe，
		// 所以接收入口不阻塞等待处理器完成；处理器也不因此自动换到另一个线程。
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
		}
		else if (message instanceof McpSchema.JSONRPCRequest request) {
			logger.debug("Received request: {}", request);
			handleIncomingRequest(request).onErrorResume(error -> {

				McpSchema.JSONRPCResponse.JSONRPCError jsonRpcError;
				if (error instanceof McpError mcpError && mcpError.getJsonRpcError() != null) {
					jsonRpcError = mcpError.getJsonRpcError();
				}
				else {
					// The cause chain may contain internal details: log it, but only send
					// the message to the server
					logger.warn("Error handling request {} [{}]", request.id(), request.method(), error);
					jsonRpcError = new McpSchema.JSONRPCResponse.JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR,
							Objects.requireNonNullElse(error.getMessage(), "Internal error"), null);
				}

				var errorResponse = McpSchema.JSONRPCResponse.error(request.id(), jsonRpcError);
				return Mono.just(errorResponse);
			}).flatMap(this.transport::sendMessage).onErrorComplete(t -> {
				if (t instanceof McpTransportSessionClosedException) {
					logger.debug("Can't send response to request {} when the transport is closed", request.id());
				}
				else if (McpTransport.isPeerClosed(t)) {
					logger.debug("Can't send response to request {}: connection closed by peer", request.id(), t);
				}
				else {
					logger.warn("Failed to send response to the server", t);
				}
				return true;
			}).subscribe();
		}
		else if (message instanceof McpSchema.JSONRPCNotification notification) {
			logger.debug("Received notification: {}", notification);
			handleIncomingNotification(notification).onErrorComplete(t -> {
				logger.error("Error handling notification: {}", t.getMessage());
				return true;
			}).subscribe();
		}
		else {
			logger.warn("Received unknown message type: {}", message);
		}
	}

	/**
	 * Handles an incoming JSON-RPC request by routing it to the appropriate handler.
	 * @param request The incoming JSON-RPC request
	 * @return A Mono containing the JSON-RPC response
	 */
	private Mono<McpSchema.JSONRPCResponse> handleIncomingRequest(McpSchema.JSONRPCRequest request) {
		return Mono.defer(() -> {
			var handler = this.requestHandlers.get(request.method());
			if (handler == null) {
				MethodNotFoundError error = getMethodNotFoundError(request.method());
				return Mono
					.just(McpSchema.JSONRPCResponse.error(request.id(), new McpSchema.JSONRPCResponse.JSONRPCError(
							McpSchema.ErrorCodes.METHOD_NOT_FOUND, error.message(), error.data())));
			}

			return handler.handle(request.params())
				.map(result -> McpSchema.JSONRPCResponse.result(request.id(), result));
		});
	}

	record MethodNotFoundError(String method, String message, Object data) {
	}

	private MethodNotFoundError getMethodNotFoundError(String method) {
		switch (method) {
			case McpSchema.METHOD_ROOTS_LIST:
				return new MethodNotFoundError(method, "Roots not supported",
						Map.of("reason", "Client does not have roots capability"));
			default:
				return new MethodNotFoundError(method, "Method not found: " + method, null);
		}
	}

	/**
	 * Handles an incoming JSON-RPC notification by routing it to the appropriate handler.
	 * @param notification The incoming JSON-RPC notification
	 * @return A Mono that completes when the notification is processed
	 */
	private Mono<Void> handleIncomingNotification(McpSchema.JSONRPCNotification notification) {
		return Mono.defer(() -> {
			var handler = notificationHandlers.get(notification.method());
			if (handler == null) {
				logger.warn("No handler registered for notification method: {}", notification);
				return Mono.empty();
			}
			return handler.handle(notification.params());
		});
	}

	/**
	 * Generates a unique request ID in a non-blocking way. Combines a session-specific
	 * prefix with an atomic counter to ensure uniqueness.
	 * @return A unique request ID string
	 */
	private String generateRequestId() {
		return this.sessionPrefix + "-" + this.requestCounter.getAndIncrement();
	}

	/**
	 * Sends a JSON-RPC request and returns the response.
	 * @param <T> The expected response type
	 * @param method The method name to call
	 * @param requestParams The request parameters
	 * @param typeRef Type reference for response deserialization
	 * @return A Mono containing the response
	 */
	@Override
	public <T> Mono<T> sendRequest(String method, Object requestParams, TypeRef<T> typeRef) {
		// ID 在调用本方法时生成，登记 sink 和发送则在订阅时发生。
		// 同一个返回 Mono 被多次订阅会复用这个 ID；它不是可随意重放的请求缓存。
		String requestId = this.generateRequestId();

		// 必须先登记再发送，才能接住很快返回的响应。这里内部订阅发送 Mono，
		// 并显式传递外层 Context，避免 HTTP 请求定制器丢失调用方上下文。
		// sendMessage 完成只表示传输发送阶段完成；RPC 的结果由入站 response 完成 sink。
		// timeout 限制调用方等待；本实现没有在取消/超时回调里移除 pendingResponses，
		// 也没有发送协议取消通知。表项在响应到达、发送失败或会话关闭时清理。
		return Mono.deferContextual(ctx -> Mono.<McpSchema.JSONRPCResponse>create(pendingResponseSink -> {
			logger.debug("Sending message for method {}", method);
			this.pendingResponses.put(requestId, pendingResponseSink);
			McpSchema.JSONRPCRequest jsonrpcRequest = new McpSchema.JSONRPCRequest(method, requestId, requestParams);
			this.transport.sendMessage(jsonrpcRequest).contextWrite(ctx).subscribe(v -> {
			}, error -> {
				this.pendingResponses.remove(requestId);
				pendingResponseSink.error(error);
			});
		})).timeout(this.requestTimeout).handle((jsonRpcResponse, deliveredResponseSink) -> {
			if (jsonRpcResponse.error() != null) {
				logger.info("Server returned a JSON-RPC error when calling method {}: {}", method,
						jsonRpcResponse.error());
				deliveredResponseSink.error(new McpError(jsonRpcResponse.error()));
			}
			else {
				if (typeRef.getType().equals(Void.class)) {
					deliveredResponseSink.complete();
				}
				else {
					deliveredResponseSink.next(this.transport.unmarshalFrom(jsonRpcResponse.result(), typeRef));
				}
			}
		});
	}

	/**
	 * Sends a JSON-RPC notification.
	 * @param method The method name for the notification
	 * @param params The notification parameters
	 * @return A Mono that completes when the notification is sent
	 */
	@Override
	public Mono<Void> sendNotification(String method, Object params) {
		McpSchema.JSONRPCNotification jsonrpcNotification = new McpSchema.JSONRPCNotification(method, params);
		return this.transport.sendMessage(jsonrpcNotification);
	}

	/**
	 * Closes the session gracefully, allowing pending operations to complete.
	 * @return A Mono that completes when the session is closed
	 */
	@Override
	public Mono<Void> closeGracefully() {
		// 实际行为是让全部待响应请求立即以错误结束，不是等待它们自然完成。
		// transport 的物理关闭由 McpAsyncClient 接着执行。
		return Mono.fromRunnable(this::dismissPendingResponses);
	}

	/**
	 * Closes the session immediately, potentially interrupting pending operations.
	 */
	@Override
	public void close() {
		dismissPendingResponses();
	}

}
