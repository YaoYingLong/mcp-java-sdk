/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.modern.server.feature;

import java.util.concurrent.Callable;

import io.modelcontextprotocol.modern.server.McpAsyncNotifier;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.server.McpAsyncResponse;
import io.modelcontextprotocol.modern.server.McpSyncNotifier;
import io.modelcontextprotocol.modern.server.McpSyncResponse;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Converts sync (blocking) repository calls and responses to their async (Reactor)
 * equivalents. Only the programming paradigm changes: a single response stays single and
 * a streaming one stays streaming. Where the sync code runs is decided per request by
 * {@link McpRequestContext#isBlocking()}: on the calling thread for a blocking caller, on
 * {@code boundedElastic} otherwise.
 *
 * @author Dariusz Jędrzejczyk
 */
// 同步仓库先适配成异步接口，但不会把单结果强制升级为流式结果。
// 线程由当前请求决定：Servlet 普通请求 handleBlocking -> isBlocking=true，保留容器线程；
// STDIO/非阻塞入口 -> boundedElastic，防止同步业务阻塞读取请求或取消通知的线程。
final class SyncAdapters {

	private SyncAdapters() {
	}

	static <O> Mono<McpAsyncResponse<O>> respond(McpRequestContext ctx, Callable<McpSyncResponse<O>> call) {
		return call(ctx, call).map(response -> toAsync(ctx, response));
	}

	static <T> Mono<T> call(McpRequestContext ctx, Callable<T> call) {
		// fromCallable 保持惰性，订阅才调用同步业务；返回 null 会完成为空，find 因此可表示未找到。
		// 仅在非阻塞请求上加 subscribeOn；不要把同步适配理解为固定切到线程池。
		Mono<T> mono = Mono.fromCallable(call);
		return ctx.isBlocking() ? mono : mono.subscribeOn(Schedulers.boundedElastic());
	}

	static McpAsyncCompletionHandler completion(McpSyncCompletionHandler handler) {
		return (ctx, request) -> call(ctx, () -> handler.complete(ctx, request));
	}

	private static <O> McpAsyncResponse<O> toAsync(McpRequestContext ctx, McpSyncResponse<O> response) {
		if (response instanceof McpSyncResponse.Result<O> result) {
			return McpAsyncResponse.result(result.result());
		}
		// 流式 body 继续延迟到 transport 真正打开流后执行，通知器才有可写的流。
		// 普通 Result 分支直接包装值，通知发送与结果类型不会因编程模型转换而改变。
		McpSyncResponse.Body<O> body = ((McpSyncResponse.Streaming<O>) response).body();
		return McpAsyncResponse.streaming(notifier -> call(ctx, () -> body.run(new BlockingSyncNotifier(notifier))));
	}

	// The async notifier's Monos complete as soon as the notification is handed to the
	// stream, so blocking on them from sync handler code is cheap.
	private static final class BlockingSyncNotifier implements McpSyncNotifier {

		private final McpAsyncNotifier delegate;

		private BlockingSyncNotifier(McpAsyncNotifier delegate) {
			this.delegate = delegate;
		}

		@Override
		public void progress(double progress, Double total, String message) {
			this.delegate.progress(progress, total, message).block();
		}

		@Override
		public void notify(String method, Object params) {
			this.delegate.notify(method, params).block();
		}

	}

}
