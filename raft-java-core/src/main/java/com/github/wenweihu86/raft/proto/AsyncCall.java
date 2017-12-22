package com.github.wenweihu86.raft.proto;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsyncCall
{
	private static final ExecutorService executor =
		Executors.newFixedThreadPool(10);

	private static <T> T interalCall(final Object target, final Method method,
		final Object[] args, final AsyncCallback<T> callback)
			throws Exception
	{
		try
		{
			T result =(T) method.invoke(target, args);

			callback.success(result);
			
			return result;
		} catch(Exception e)
		{
			callback.fail(e);
			throw e;
		}
	}

	public static <T> Future<T> call(
		final Object target, final String method,
		final Object[] args, final AsyncCallback<T> callback)
	{
		return executor.submit(new Callable<T>() {
			@Override
			public T call()
				throws Exception
			{
				Class[] argTypes = new Class[args.length];
				for(int index = 0; index != args.length; ++index)
					argTypes[index] = args[index].getClass();

				Method infactMethod =
					target.getClass().getMethod(method, argTypes);
				
				return interalCall(target, infactMethod, args, callback);
			}
		});
	}
	
	public static <T> Future<T> call(
		final Object target, final Method method,
		final Object[] args, final AsyncCallback<T> callback)
	{
		return executor.submit(new Callable<T>() {
			@Override
			public T call()
				throws Exception
			{
				return interalCall(target, method, args, callback);
			}
		});
	}
}
