package com.github.lisama569.autoremediationorchestratorjava.engine;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 带超时的动作执行器。
 *
 * <p>每个动作在独立虚拟线程中执行，调用方线程用 {@code Future.get(timeout)}
 * 等待；超时后向工作线程发中断。模拟动作必须尊重中断（使用可中断阻塞），
 * 以便超时场景在测试中快速、确定性地收敛。</p>
 */
public final class ActionExecutor {

    private final ExecutorService pool =
            Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 在 timeoutMs 内执行 task。
     *
     * @throws TimeoutRuntimeException 动作超时
     * @throws RuntimeException       动作自身抛出的异常（原样透传）
     */
    public void runWithTimeout(String label, long timeoutMs, ThrowingTask task) {
        Future<?> future = pool.submit(() -> {
            task.run();
            return null;
        });
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new TimeoutRuntimeException(label + " timed out after " + timeoutMs + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(label + " failed: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(label + " interrupted", e);
        }
    }

    public void shutdown() {
        pool.shutdownNow();
    }

    /** 可抛受检异常的任务。 */
    @FunctionalInterface
    public interface ThrowingTask {
        void run() throws Exception;
    }
}
