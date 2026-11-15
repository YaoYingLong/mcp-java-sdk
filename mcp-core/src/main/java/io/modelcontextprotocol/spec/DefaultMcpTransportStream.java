/*
 * Copyright 2024-2025 the original author or authors.
 */

package io.modelcontextprotocol.spec;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * An implementation of {@link McpTransportStream} using Project Reactor types.
 *
 * @param <CONNECTION> the resource serving the stream
 * @author Dariusz Jędrzejczyk
 */
public class DefaultMcpTransportStream<CONNECTION> implements McpTransportStream<CONNECTION> {

	// 中文导读：一个实例跟踪一条可恢复 SSE 流。lastId 是 SSE 事件 id，不是 JSON-RPC 请求 id；
	// reconnect 回调由具体 transport 提供，本类只保存游标和触发恢复，不保存可回放消息。

	private static final Logger logger = LoggerFactory.getLogger(DefaultMcpTransportStream.class);

	private static final AtomicLong counter = new AtomicLong();

	private final AtomicReference<String> lastId = new AtomicReference<>();

	// Used only for internal accounting
	private final long streamId;

	private final boolean resumable;

	private final Function<McpTransportStream<CONNECTION>, Publisher<CONNECTION>> reconnect;

	/**
	 * Constructs a new instance representing a particular stream that can resume using
	 * the provided reconnect mechanism.
	 * @param resumable whether the stream is resumable and should try to reconnect
	 * @param reconnect the mechanism to use in case an error is observed on the current
	 * event stream to asynchronously kick off a resumed stream consumption, potentially
	 * using the stored {@link #lastId()}.
	 */
	public DefaultMcpTransportStream(boolean resumable,
			Function<McpTransportStream<CONNECTION>, Publisher<CONNECTION>> reconnect) {
		this.reconnect = reconnect;
		this.streamId = counter.getAndIncrement();
		this.resumable = resumable;
	}

	@Override
	public Optional<String> lastId() {
		return Optional.ofNullable(this.lastId.get());
	}

	@Override
	public long streamId() {
		return this.streamId;
	}

	@Override
	public Publisher<McpSchema.JSONRPCMessage> consumeSseStream(
			Publisher<Tuple2<Optional<String>, Iterable<McpSchema.JSONRPCMessage>>> eventStream) {

		// @formatter:off
		// 先记录事件 id，再展开事件中的消息。错误时另起恢复订阅，并把当前 Reactor Context 传过去；
		// 原流仍会向下游报告错误，这不是 retry 操作符。会话已失效时不尝试恢复同一流。
		return Flux.deferContextual(ctx -> Flux.from(eventStream)
			.doOnNext(idAndMessage -> idAndMessage.getT1().ifPresent(id -> {
				String previousId = this.lastId.getAndSet(id);
				logger.debug("Updating last id {} -> {} for stream {}", previousId, id, this.streamId);
			}))
			.doOnError(e -> {
				if (resumable && !(e instanceof McpTransportSessionNotFoundException)) {
					Mono.from(reconnect.apply(this)).contextWrite(ctx).subscribe();
				}
			})
			.flatMapIterable(Tuple2::getT2)); // @formatter:on
	}

}
