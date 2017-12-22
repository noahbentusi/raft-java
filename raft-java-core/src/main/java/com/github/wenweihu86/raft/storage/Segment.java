package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.util.RaftFileUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class Segment
{
    public static class Record
    {
        public int offset;
        public int size;

        public Record(int offset, int size)
        {
            this.offset = offset;
            this.size = size;
        }
    }

    private boolean canWrite;

    private long startIndex;
    private long endIndex;

    private String storageName;
    private String key;
    private int length;

    public String getStorageName() {
		return storageName;
	}

	public void setStorageName(String storageName) {
		this.storageName = storageName;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	private List<Record> entries = new ArrayList<>();

    public RaftMessage.LogEntry getEntry(long index)
    {
        if (startIndex == 0 || endIndex == 0)
        {
            return null;
        }
        
        if (index < startIndex || index > endIndex)
        {
            return null;
        }

        int indexInList = (int) (index - startIndex);

        Record record = entries.get(indexInList);
        
        byte[] data = null;
        
        Storage storage = Snapshot.storageFactory.getStorage(storageName);
        
        try
        {
        	data = storage.get(key, record.offset, record.size);	
        } finally
        {
        	storage.close();
        }
        
        return RaftFileUtils.readProtoFromFile(
        	new DataInputStream(new ByteArrayInputStream(data)),
        	data.length, RaftMessage.LogEntry.class);
    }

    public boolean isCanWrite()
    {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite)
    {
        this.canWrite = canWrite;
    }

    public long getStartIndex()
    {
        return startIndex;
    }

    public void setStartIndex(long startIndex)
    {
        this.startIndex = startIndex;
    }

    public long getEndIndex()
    {
        return endIndex;
    }

    public void setEndIndex(long endIndex)
    {
        this.endIndex = endIndex;
    }

    public List<Record> getEntries()
    {
        return entries;
    }

    public void setEntries(List<Record> entries)
    {
        this.entries = entries;
    }
}
