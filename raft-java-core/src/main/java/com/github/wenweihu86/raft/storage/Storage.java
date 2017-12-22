package com.github.wenweihu86.raft.storage;

import java.util.List;

public interface Storage
{
	public List<String> getKeys();

	public void put(String key, byte[] data);
	public void append(String key, byte[] data);

	public byte[] get(String key);
	public byte[] get(String key, int offset, int size);

	public byte[] truncate(String key, int size);

	public void rename(String oldKey, String newKey);
	public void delete(String key);

	public void close();
}
