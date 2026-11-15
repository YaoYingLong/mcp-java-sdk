/*
 * Copyright 2024-2024 the original author or authors.
 */

package io.modelcontextprotocol.client.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Implementation of the MCP Stdio transport that communicates with a server process using
 * standard input/output streams. Messages are exchanged as newline-delimited JSON-RPC
 * messages over stdin/stdout, with errors and debug information sent to stderr.
 *
 * @author Christian Tzolov
 * @author Dariusz Jędrzejczyk
 */
// 客户端拥有并启动服务端子进程：向其 stdin 写请求，从 stdout 按行读取协议消息，
// stderr 单独作为诊断输出。stdout 上混入普通日志会被当作 JSON-RPC 解析而失败。
// 入站、出站、错误流各有单线程调度器；业务 API 使用 Mono 并不意味着阻塞式管道读取消失，
// 而是由这些专用线程承担。关闭传输也负责终止子进程和释放这三个调度器。
public class StdioClientTransport implements McpClientTransport {

	private static final Logger logger = LoggerFactory.getLogger(StdioClientTransport.class);

	private static final int DEFAULT_INPUT_MAX_SIZE = 16 * 1024 * 1024; // 16MB

	// @formatter:off
	private static final Set<Integer> EXIT_SUCCESS_CODES = Set.of(
			0,   // success
			130, // interrupted (SIGINT)
			141, // pipeline shortcut (SIGPIPE)
			143  // graceful termination (SIGTERM)
	);
	// @formatter:on

	private final Sinks.Many<JSONRPCMessage> inboundSink;

	private final Sinks.Many<JSONRPCMessage> outboundSink;

	/** The server process being communicated with */
	private Process process;

	private McpJsonMapper jsonMapper;

	/** Scheduler for handling inbound messages from the server process */
	private Scheduler inboundScheduler;

	/** Scheduler for handling outbound messages to the server process */
	private Scheduler outboundScheduler;

	/** Scheduler for handling error messages from the server process */
	private Scheduler errorScheduler;

	/** Parameters for configuring and starting the server process */
	private final ServerParameters params;

	private final Sinks.Many<String> errorSink;

	private final int inputMaxSize;

	private volatile boolean isClosing = false;

	// visible for tests
	private Consumer<String> stdErrorHandler = error -> logger.info("STDERR Message received: {}", error);

	/**
	 * Creates a new StdioClientTransport with the specified parameters and JsonMapper.
	 * @param params The parameters for configuring the server process
	 * @param jsonMapper The JsonMapper to use for JSON serialization/deserialization
	 */
	public StdioClientTransport(ServerParameters params, McpJsonMapper jsonMapper) {
		this(params, jsonMapper, DEFAULT_INPUT_MAX_SIZE);
	}

	/**
	 * Creates a new StdioClientTransport with the specified parameters and JsonMapper.
	 * @param params The parameters for configuring the server process
	 * @param jsonMapper The JsonMapper to use for JSON serialization/deserialization
	 * @param inputMaxSize The maximum number of characters read for a single inbound
	 * message. A peer that sends a longer message (or never terminates a line) has its
	 * message rejected instead of forcing the transport to buffer it in memory.
	 */
	public StdioClientTransport(ServerParameters params, McpJsonMapper jsonMapper, int inputMaxSize) {
		Assert.notNull(params, "The params can not be null");
		Assert.notNull(jsonMapper, "The JsonMapper can not be null");
		Assert.isTrue(inputMaxSize > 0, "inputMaxSize must be positive");

		this.inboundSink = Sinks.many().unicast().onBackpressureBuffer();
		this.outboundSink = Sinks.many().unicast().onBackpressureBuffer();

		this.params = params;

		this.jsonMapper = jsonMapper;

		this.inputMaxSize = inputMaxSize;

		this.errorSink = Sinks.many().unicast().onBackpressureBuffer();

		// Start threads
		this.inboundScheduler = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor(), "inbound");
		this.outboundScheduler = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor(), "outbound");
		this.errorScheduler = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor(), "error");
	}

	/**
	 * Starts the server process and initializes the message processing streams. This
	 * method sets up the process with the configured command, arguments, and environment,
	 * then starts the inbound, outbound, and error processing threads.
	 * @throws RuntimeException if the process fails to start or if the process streams
	 * are null
	 */
	@Override
	public Mono<Void> connect(Function<Mono<JSONRPCMessage>, Mono<JSONRPCMessage>> handler) {
		// fromRunnable 在订阅时启动进程，subscribeOn 将启动动作移到 boundedElastic。
		// command 与 args 分别作为 ProcessBuilder 的参数列表传入，不经 shell 拼接解释；
		// env 覆盖到 ProcessBuilder 继承的环境中。三个长期读取/写入任务随后单独启动。
		return Mono.<Void>fromRunnable(() -> {
			logger.info("MCP server starting.");
			handleIncomingMessages(handler);
			handleIncomingErrors();

			// Prepare command and environment
			List<String> fullCommand = new ArrayList<>();
			fullCommand.add(params.getCommand());
			fullCommand.addAll(params.getArgs());

			ProcessBuilder processBuilder = this.getProcessBuilder();
			processBuilder.command(fullCommand);
			processBuilder.environment().putAll(params.getEnv());

			// Start the process
			try {
				this.process = processBuilder.start();
			}
			catch (IOException e) {
				throw new RuntimeException("Failed to start process with command: " + fullCommand, e);
			}

			// Validate process streams
			if (this.process.getInputStream() == null || process.getOutputStream() == null) {
				this.process.destroy();
				throw new RuntimeException("Process input or output stream is null");
			}

			// Start threads
			startInboundProcessing();
			startOutboundProcessing();
			startErrorProcessing();
			logger.info("MCP server started");
		}).subscribeOn(Schedulers.boundedElastic());
	}

	/**
	 * Creates and returns a new ProcessBuilder instance. Protected to allow overriding in
	 * tests.
	 * @return A new ProcessBuilder instance
	 */
	protected ProcessBuilder getProcessBuilder() {
		return new ProcessBuilder();
	}

	/**
	 * Sets the handler for processing transport-level errors.
	 *
	 * <p>
	 * The provided handler will be called when errors occur during transport operations,
	 * such as connection failures or protocol violations.
	 * </p>
	 * @param errorHandler a consumer that processes error messages
	 */
	public void setStdErrorHandler(Consumer<String> errorHandler) {
		this.stdErrorHandler = errorHandler;
	}

	/**
	 * Waits for the server process to exit.
	 * @throws RuntimeException if the process is interrupted while waiting
	 */
	public void awaitForExit() {
		try {
			this.process.waitFor();
		}
		catch (InterruptedException e) {
			throw new RuntimeException("Process interrupted", e);
		}
	}

	/**
	 * Starts the error processing thread that reads from the process's error stream.
	 * Error messages are logged and emitted to the error sink.
	 */
	private void startErrorProcessing() {
		this.errorScheduler.schedule(() -> {
			try (BufferedReader processErrorReader = new BufferedReader(
					new InputStreamReader(process.getErrorStream()))) {
				String line;
				while (!isClosing && (line = processErrorReader.readLine()) != null) {
					try {
						if (!this.errorSink.tryEmitNext(line).isSuccess()) {
							if (!isClosing) {
								logger.error("Failed to emit error message");
							}
							break;
						}
					}
					catch (Exception e) {
						if (!isClosing) {
							logger.error("Error processing error message", e);
						}
						break;
					}
				}
			}
			catch (IOException e) {
				if (!isClosing) {
					logger.error("Error reading from error stream", e);
				}
			}
			finally {
				isClosing = true;
				errorSink.tryEmitComplete();
			}
		});
	}

	private void handleIncomingMessages(Function<Mono<JSONRPCMessage>, Mono<JSONRPCMessage>> inboundMessageHandler) {
		this.inboundSink.asFlux()
			.flatMap(message -> Mono.just(message)
				.transform(inboundMessageHandler)
				.contextWrite(ctx -> ctx.put("observation", "myObservation")))
			.subscribe();
	}

	private void handleIncomingErrors() {
		this.errorSink.asFlux().subscribe(e -> {
			this.stdErrorHandler.accept(e);
		});
	}

	@Override
	public Mono<Void> sendMessage(JSONRPCMessage message) {
		// 注意这个实现的入队发生在方法调用时，不是返回 Mono 被订阅时。
		// Mono.empty 只表示入队成功，并不等待下面出站线程写完或对端返回 response。
		// session 会先登记 pendingResponses，再调用此方法，避免快速响应丢失关联。
		try {
			// busyLooping retries FAIL_NON_SERIALIZED under concurrent senders
			this.outboundSink.emitNext(message, Sinks.EmitFailureHandler.busyLooping(Duration.ofMillis(100)));
			return Mono.empty();
		}
		catch (Sinks.EmissionException e) {
			return Mono.error(new RuntimeException("Failed to enqueue message", e));
		}
	}

	/**
	 * Starts the inbound processing thread that reads JSON-RPC messages from the
	 * process's input stream. Messages are deserialized and emitted to the inbound sink.
	 */
	private void startInboundProcessing() {
		// 一行就是一条消息；readLine 在积累字符的过程中检查上限，不能先用无界
		// BufferedReader.readLine 再检查长度，否则缺少换行的输入已经可能占满内存。
		// 解析完成后进入 inboundSink，再经 connect 注册的 handler 到达会话分发器。
		this.inboundScheduler.schedule(() -> {
			try (BufferedReader processReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String line;
				while (!isClosing && (line = readLine(processReader, inputMaxSize)) != null) {
					try {
						JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(this.jsonMapper, line);
						if (!this.inboundSink.tryEmitNext(message).isSuccess()) {
							if (!isClosing) {
								logger.error("Failed to enqueue inbound message: {}", message);
							}
							break;
						}
					}
					catch (Exception e) {
						if (!isClosing) {
							logger.error("Error processing inbound message for line: {}", line, e);
						}
						break;
					}
				}
			}
			catch (MaxSizeExceededException e) {
				if (!isClosing) {
					logger.error("Inbound message exceeds the maximum allowed size", e);
				}
			}
			catch (IOException e) {
				if (!isClosing) {
					logger.error("Error reading from input stream", e);
				}
			}
			finally {
				isClosing = true;
				inboundSink.tryEmitComplete();
			}
		});
	}

	/**
	 * Reads a single line, mirroring {@link BufferedReader#readLine()}, but aborting once
	 * more than {@code maxSize} characters have been read without encountering a line
	 * terminator. This bounds how much memory a single inbound message can occupy.
	 */
	static String readLine(BufferedReader reader, int maxSize) throws IOException, MaxSizeExceededException {
		StringBuilder sb = new StringBuilder();
		int c;
		while ((c = reader.read()) != -1) {
			if (c == '\n') {
				return sb.toString();
			}
			if (c == '\r') {
				// Consume an optional trailing '\n' so that "\r\n" is treated as a
				// single terminator, mirroring BufferedReader#readLine().
				reader.mark(1);
				int next = reader.read();
				if (next != '\n' && next != -1) {
					reader.reset();
				}
				return sb.toString();
			}
			if (sb.length() >= maxSize) {
				throw new MaxSizeExceededException(
						"Inbound message exceeds the maximum allowed size of " + maxSize + " characters");
			}
			sb.append((char) c);
		}
		return sb.isEmpty() ? null : sb.toString();
	}

	/**
	 * Starts the outbound processing thread that writes JSON-RPC messages to the
	 * process's output stream. Messages are serialized to JSON and written with a newline
	 * delimiter.
	 */
	private void startOutboundProcessing() {
		// publishOn 是这里真正切换写入线程的边界；多调用方先入队，实际序列化、
		// 写 stdin、补换行与 flush 在 outboundScheduler 上完成。
		this.handleOutbound(messages -> messages
			// this bit is important since writes come from user threads, and we
			// want to ensure that the actual writing happens on a dedicated thread
			.publishOn(outboundScheduler)
			.handle((message, s) -> {
				if (message != null && !isClosing) {
					try {
						String jsonMessage = jsonMapper.writeValueAsString(message);
						// Escape any embedded newlines in the JSON message as per spec:
						// https://spec.modelcontextprotocol.io/specification/basic/transports/#stdio
						// - Messages are delimited by newlines, and MUST NOT contain
						// embedded newlines.
						jsonMessage = jsonMessage.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n");

						var os = this.process.getOutputStream();
						synchronized (os) {
							os.write(jsonMessage.getBytes(StandardCharsets.UTF_8));
							os.write("\n".getBytes(StandardCharsets.UTF_8));
							os.flush();
						}
						s.next(message);
					}
					catch (IOException e) {
						s.error(new RuntimeException(e));
					}
				}
			}));
	}

	protected void handleOutbound(Function<Flux<JSONRPCMessage>, Flux<JSONRPCMessage>> outboundConsumer) {
		outboundConsumer.apply(outboundSink.asFlux()).doOnComplete(() -> {
			isClosing = true;
			outboundSink.tryEmitComplete();
		}).doOnError(e -> {
			if (!isClosing) {
				logger.error("Error in outbound processing", e);
				isClosing = true;
				outboundSink.tryEmitComplete();
			}
		}).subscribe();
	}

	/**
	 * Gracefully closes the transport by destroying the process and disposing of the
	 * schedulers. This method sends a TERM signal to the process and waits for it to exit
	 * before cleaning up resources.
	 * @return A Mono that completes when the transport is closed
	 */
	@Override
	public Mono<Void> closeGracefully() {
		// 订阅后先置关闭标志并完成各 sink，再短暂延迟、destroy 子进程、等待 onExit。
		// 这条链没有在本方法内设置等待进程退出的总超时；同步客户端在外层限制关闭等待。
		// 最后硬 dispose 调度器，以中断仍可能阻塞在管道读取上的任务。
		return Mono.fromRunnable(() -> {
			isClosing = true;
			logger.debug("Initiating graceful shutdown");
		}).then(Mono.<Void>defer(() -> {
			// First complete all sinks to stop accepting new messages
			inboundSink.tryEmitComplete();
			outboundSink.tryEmitComplete();
			errorSink.tryEmitComplete();

			// Give a short time for any pending messages to be processed
			return Mono.delay(Duration.ofMillis(100)).then();
		})).then(Mono.defer(() -> {
			logger.debug("Sending TERM to process");
			if (this.process != null) {
				this.process.destroy();
				return Mono.fromFuture(process.onExit());
			}
			else {
				logger.warn("Process not started");
				return Mono.<Process>empty();
			}
		})).doOnNext(process -> {
			int exitValue = process.exitValue();
			if (EXIT_SUCCESS_CODES.contains(exitValue)) {
				logger.info("MCP server completed successfully with code {}", exitValue);
			}
			else {
				logger.warn("MCP server process failed with code {}", exitValue);
			}
		}).then(Mono.fromRunnable(() -> {
			try {
				// The Threads are blocked on readLine so disposeGracefully would not
				// interrupt them, therefore we issue an async hard dispose.
				inboundScheduler.dispose();
				errorScheduler.dispose();
				outboundScheduler.dispose();

				logger.debug("Graceful shutdown completed");
			}
			catch (Exception e) {
				logger.error("Error during graceful shutdown", e);
			}
		})).then().subscribeOn(Schedulers.boundedElastic());
	}

	public Sinks.Many<String> getErrorSink() {
		return this.errorSink;
	}

	@Override
	public <T> T unmarshalFrom(Object data, TypeRef<T> typeRef) {
		return this.jsonMapper.convertValue(data, typeRef);
	}

}
