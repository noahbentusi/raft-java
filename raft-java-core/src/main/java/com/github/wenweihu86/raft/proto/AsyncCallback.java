package com.github.wenweihu86.raft.proto;

public interface AsyncCallback<T>
{
	public void success(T response);
	public void fail(Throwable e);
}
