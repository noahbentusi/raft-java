package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.proto.ProtocolFactory;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wenweihu86 on 2017/5/6.
 */
public class Snapshot
{
    public class SnapshotDataFile
    {
    	public String storageName;
        public String key;
        public int length;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
 
    public static final StorageFactory storageFactory;
    
    static {
    	ServiceLoader<StorageFactory> loader =
    		ServiceLoader.load(StorageFactory.class);
    	
    	storageFactory = loader.iterator().next();
    }
    
    private String snapshotDir;
    
    private RaftMessage.SnapshotMetaData metaData;
    
    // 表示是否正在安装snapshot，leader向follower安装，leader和follower同时处于installSnapshot状态
    private AtomicBoolean isInstallSnapshot = new AtomicBoolean(false);
    
    // 表示节点自己是否在对状态机做snapshot
    private AtomicBoolean isTakeSnapshot = new AtomicBoolean(false);

    private Lock lock = new ReentrantLock();

    public Snapshot(String raftDataDir)
    {
        this.snapshotDir = raftDataDir + "-" + "snapshot";
    }

    public void reload()
    {
        metaData = readMetaData();
        if (metaData == null)
        {
            metaData =
            	RaftMessage.SnapshotMetaData.newBuilder().build();
        }
    }

    /**
     * 打开snapshot data目录下的文件，
     * 如果是软链接，需要打开实际文件句柄
     * @return 文件名以及文件句柄map
     */
    public TreeMap<String, SnapshotDataFile> openSnapshotDataFiles()
    {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();
        String snapshotDataDir = snapshotDir + "-" + "data";
        
    	Storage storage =
    		storageFactory.getStorage(snapshotDataDir);
    	
    	List<String> fileNames = storage.getKeys();
        
    	for(String key : fileNames)
        {
            SnapshotDataFile snapshotFile = new SnapshotDataFile();
            
            snapshotFile.storageName = snapshotDataDir;
            snapshotFile.key = key;
            snapshotFile.length = storage.get(key).length;
        }

    	storage.close();

        return snapshotDataFileMap;
    }

    public void closeSnapshotDataFiles(
    	TreeMap<String, SnapshotDataFile> snapshotDataFileMap)
    { }

    public RaftMessage.SnapshotMetaData readMetaData()
    {
        String fileName = snapshotDir + "-" + "metadata";
        String key = "metadata";
        
        Storage storage = storageFactory.getStorage(fileName);
        
        try
        {
	        byte[] data = storage.get(key);
	        if (data == null)
	        	return null;
	        
	        RaftMessage.SnapshotMetaData metadata =
	        	RaftFileUtils.readProtoFromFile(
	                new DataInputStream(new ByteArrayInputStream(data)),
	                data.length, RaftMessage.SnapshotMetaData.class);

	        return metadata;
        } finally
        {
        	storage.close();
        }
    }

    public void updateMetaData(String dir,
	       Long lastIncludedIndex,
	       Long lastIncludedTerm,
	       RaftMessage.Configuration configuration)
    {
        RaftMessage.SnapshotMetaData snapshotMetaData =
        	RaftMessage.SnapshotMetaData.newBuilder()
                .setLastIncludedIndex(lastIncludedIndex)
                .setLastIncludedTerm(lastIncludedTerm)
                .setConfiguration(configuration).build();

        String fileName = dir + "-" + "metadata";
        String key = "metadata";

        Storage storage = storageFactory.getStorage(fileName);

        try
        {
        	ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        	
            RaftFileUtils.writeProtoToFile(
            	new DataOutputStream(byteOutput), snapshotMetaData);
            
            storage.put(key, byteOutput.toByteArray());
        } finally
        {
        	storage.close();
        }
    }

    public RaftMessage.SnapshotMetaData getMetaData()
    {
        return metaData;
    }

    public String getSnapshotDir()
    {
        return snapshotDir;
    }

    public AtomicBoolean getIsInstallSnapshot()
    {
        return isInstallSnapshot;
    }

    public AtomicBoolean getIsTakeSnapshot()
    {
        return isTakeSnapshot;
    }

    public Lock getLock()
    {
        return lock;
    }
}
