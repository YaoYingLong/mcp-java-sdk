/*
 * Copyright 2024-2025 the original author or authors.
 */

package io.modelcontextprotocol.spec;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Default implementation of {@link McpTransportSession} which manages the open
 * connections using tye {@link Disposable} type and allows to perform clean up using the
 * {@link Disposable#dispose()} method.
 *
 * @author Dariusz Jędrzejczyk
 */
public class DefaultMcpTransportSession implements McpTransportSession<Disposable> {

	// 中文导读：这是 HTTP 客户端的传输会话资源容器，不是处理 JSON-RPC 方法的 McpClientSession。
	// 一个逻辑会话可以持有多个连接订阅，sessionId 用于 HTTP 请求中的会话关联。

	private static final Logger logger = LoggerFactory.getLogger(DefaultMcpTransportSession.class);

	private final Disposable.Composite openConnections = Disposables.composite();

	private final AtomicBoolean initialized = new AtomicBoolean(false);

	private final AtomicReference<String> sessionId = new AtomicReference<>();

	private final Function<String, Publisher<Void>> onClose;

	public DefaultMcpTransportSession(Function<String, Publisher<Void>> onClose) {
		this.onClose = onClose;
	}

	@Override
	public Optional<String> sessionId() {
		return Optional.ofNullable(this.sessionId.get());
	}

	@Override
	public boolean markInitialized(String sessionId) {
		// initialized 单独记录初始化状态，因为服务端可以不分配 sessionId；null 不代表尚未初始化。
		// 只接受第一次的会话 id，后续响应给出不同 id 时记录警告而不静默替换。
		boolean flipped = this.initialized.compareAndSet(false, true);
		if (flipped) {
			this.sessionId.set(sessionId);
			logger.debug("Established session with id {}", sessionId);
		}
		else {
			if (sessionId != null && !sessionId.equals(this.sessionId.get())) {
				logger.warn("Different session id provided in response. Expecting {} but server returned {}",
						this.sessionId.get(), sessionId);
			}
		}
		return flipped;
	}

	@Override
	public void addConnection(Disposable connection) {
		this.openConnections.add(connection);
	}

	@Override
	public void removeConnection(Disposable connection) {
		this.openConnections.remove(connection);
	}

	@Override
	public void close() {
		this.closeGracefully().subscribe();
	}

	@Override
	public Mono<Void> closeGracefully() {
		// 先由 transport 提供的回调执行远端关闭，再释放本地连接；回调失败也释放连接并保留错误。
		return Mono.from(this.onClose.apply(this.sessionId.get()))
			.onErrorResume(error -> Mono.fromRunnable(this.openConnections::dispose).then(Mono.error(error)))
			.then(Mono.fromRunnable(this.openConnections::dispose));
	}

}
