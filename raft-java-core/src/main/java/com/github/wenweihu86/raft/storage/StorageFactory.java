package com.github.wenweihu86.raft.storage;

import java.util.List;

public interface StorageFactory
{
	public List<String> getStorageNames();

	public Storage getStorage(String name);

	public void renameStorage(String oldName, String newName);
}
