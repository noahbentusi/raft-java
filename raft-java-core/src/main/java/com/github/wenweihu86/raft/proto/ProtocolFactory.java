package com.github.wenweihu86.raft.proto;

public interface ProtocolFactory
{
	public void publish(RaftMessage.Server server, Object service);
	
	public RemoteClient connect(RaftMessage.Server server);

	public <T> T getProxy(RemoteClient client, Class<T> clazz);
}
