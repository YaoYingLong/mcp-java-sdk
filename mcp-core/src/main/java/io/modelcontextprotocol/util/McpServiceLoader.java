/**
 * Copyright 2026 - 2026 the original author or authors.
 */
package io.modelcontextprotocol.util;

import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * Instance of this class are intended to be used differently in OSGi and non-OSGi
 * environments. In all non-OSGi environments the supplier member will be
 * <code>null</code> and the serviceLoad method will be called to use the
 * ServiceLoader.load to find the first instance of the supplier (assuming one is present
 * in the runtime), cache it, and call the supplier's get method.
 * <p>
 * In OSGi environments, the Service component runtime (scr) will call the setSupplier
 * method upon bundle activation (assuming one is present in the runtime), and subsequent
 * calls will use the given supplier instance rather than the ServiceLoader.load.
 *
 * @param <S> the type of the supplier
 * @param <R> the type of the supplier result/returned value
 */
public class McpServiceLoader<S extends Supplier<R>, R> {

	private Class<S> supplierType;

	private S supplier;

	private R supplierResult;

	public void setSupplier(S supplier) {
		this.supplier = supplier;
		this.supplierResult = null;
	}

	public void unsetSupplier(S supplier) {
		this.supplier = null;
		this.supplierResult = null;
	}

	public McpServiceLoader(Class<S> supplierType) {
		this.supplierType = supplierType;
	}

	protected Optional<S> serviceLoad(Class<S> type) {
		// 普通 JVM 使用 ServiceLoader 的默认类加载机制查找 provider；这里只取第一个实现。
		// 若同时放入多个 JSON provider，不应把 classpath 中的发现顺序当作显式的版本选择配置。
		return ServiceLoader.load(type).findFirst();
	}

	@SuppressWarnings("unchecked")
	public synchronized R getDefault() {
		// 同一加载器缓存 supplier 及其产物，避免每次取默认 mapper 都重新构建。
		// setSupplier/unsetSupplier 会清空产物缓存，供 OSGi 服务绑定变化后重新获取。
		if (this.supplierResult == null) {
			if (this.supplier == null) {
				// Use serviceloader
				Optional<?> sl = serviceLoad(this.supplierType);
				if (sl.isEmpty()) {
					throw new ServiceConfigurationError(
							"No %s available for creating McpJsonMapper".formatted(this.supplierType.getSimpleName()));
				}
				this.supplier = (S) sl.get();
			}
			this.supplierResult = this.supplier.get();
		}
		return supplierResult;
	}

}
