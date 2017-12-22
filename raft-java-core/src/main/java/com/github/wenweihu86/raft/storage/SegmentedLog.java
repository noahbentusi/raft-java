package com.github.wenweihu86.raft.storage;

import com.github.wenweihu86.raft.RaftOptions;
import com.github.wenweihu86.raft.util.RaftFileUtils;
import com.github.wenweihu86.raft.proto.RaftMessage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by wenweihu86 on 2017/5/3.
 */
public class SegmentedLog
{
    private static Logger LOG =
    	LoggerFactory.getLogger(SegmentedLog.class);

    private String logDir;
    private String logDataDir;
    
    private int maxSegmentFileSize;
    
    private RaftMessage.LogMetaData metaData;
    
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();

    // segment log占用的内存大小，用于判断是否需要做snapshot
    private volatile long totalSize;

    public SegmentedLog(String raftDataDir, int maxSegmentFileSize)
    {
        this.logDir = raftDataDir + "-" + "log";
        this.logDataDir = logDir + "-" + "data";
        
        this.maxSegmentFileSize = maxSegmentFileSize;
        
        readSegments();

        for (Segment segment: startLogIndexSegmentMap.values())
        {
            this.loadSegmentData(segment);
        }

        metaData = this.readMetaData();
        
        if (metaData == null)
        {
            if (startLogIndexSegmentMap.size() > 0)
            {
                LOG.error("No readable metadata file but found segments in {}", logDir);
                
                throw new RuntimeException("No readable metadata file but found segments");
            }
            
            metaData =
            	RaftMessage.LogMetaData.newBuilder()
            		.setFirstLogIndex(1).build();
        }
    }

    public RaftMessage.LogEntry getEntry(long index)
    {
        long firstLogIndex = getFirstLogIndex();
        long lastLogIndex = getLastLogIndex();
        
        if (index == 0 || index < firstLogIndex || index > lastLogIndex)
        {
            LOG.debug("index out of range, index={}, firstLogIndex={}, lastLogIndex={}",
                index, firstLogIndex, lastLogIndex);

            return null;
        }

        if (startLogIndexSegmentMap.size() == 0)
        {
            return null;
        }

        Segment segment =
        	startLogIndexSegmentMap.floorEntry(index).getValue();

        return segment.getEntry(index);
    }

    public long getEntryTerm(long index)
    {
        RaftMessage.LogEntry entry = getEntry(index);

        if (entry == null)
        {
            return 0;
        }

        return entry.getTerm();
    }

    public long getFirstLogIndex()
    {
        return metaData.getFirstLogIndex();
    }

    public long getLastLogIndex()
    {
        // 有两种情况segment为空
        // 1、第一次初始化，firstLogIndex = 1，lastLogIndex = 0
        // 2、snapshot刚完成，日志正好被清理掉，firstLogIndex = snapshotIndex + 1， lastLogIndex = snapshotIndex
        if (startLogIndexSegmentMap.size() == 0)
        {
            return getFirstLogIndex() - 1;
        }

        Segment lastSegment =
        	startLogIndexSegmentMap.lastEntry().getValue();

        return lastSegment.getEndIndex();
    }

    public long append(List<RaftMessage.LogEntry> entries)
    {
        long newLastLogIndex = this.getLastLogIndex();

        for(RaftMessage.LogEntry entry: entries)
        {
            newLastLogIndex++;
            
            int entrySize = entry.getSerializedSize();
            int segmentSize = startLogIndexSegmentMap.size();
            
            boolean isNeedNewSegmentFile = false;
            

            if (segmentSize == 0)
            {
                isNeedNewSegmentFile = true;
            } else
            {
                Segment segment =
                	startLogIndexSegmentMap.lastEntry().getValue();
                
                if (!segment.isCanWrite())
                {
                    isNeedNewSegmentFile = true;
                } else
                if (segment.getLength() + entrySize >= maxSegmentFileSize)
                {
                    isNeedNewSegmentFile = true;

                    // 最后一个segment的文件close并改名
                    segment.setCanWrite(false);

                    String newFileName =
                    	String.format("%020d-%020d",
                                segment.getStartIndex(), segment.getEndIndex());
                        
                        String newFullFileName = newFileName;
                        String oldFullFileName = segment.getKey();
 
                        Storage storage =
                        	Snapshot.storageFactory.getStorage(
                        		segment.getStorageName());
                        
                        try
                        {
                        	storage.rename(oldFullFileName, newFullFileName);
                        } finally
                        {
                        	storage.close();
                        }

                        segment.setKey(newFullFileName);
                    }
                }

                Segment newSegment;

                // 新建segment文件
            if (isNeedNewSegmentFile)
            {
                // open new segment file
                String newSegmentFileName =
                	String.format("open-%d", newLastLogIndex);
                String newFullFileName = newSegmentFileName;

                Segment segment = new Segment();

                segment.setStorageName(logDataDir);
                segment.setCanWrite(true);
                segment.setStartIndex(newLastLogIndex);
                segment.setEndIndex(0);
                segment.setKey(newSegmentFileName);
                segment.setLength(0);

                newSegment = segment;
            } else
            {
                newSegment =
                	startLogIndexSegmentMap.lastEntry().getValue();
            }

            // 写proto到segment中
            if (entry.getIndex() == 0)
            {
                entry =
                	RaftMessage.LogEntry.newBuilder(entry)
                        .setIndex(newLastLogIndex).build();
            }

            newSegment.setEndIndex(entry.getIndex());
            
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            
            RaftFileUtils.writeProtoToFile(
            	new DataOutputStream(output), entry);
            
            byte[] data = output.toByteArray();
            
            newSegment.setEndIndex(entry.getIndex());
            newSegment.getEntries().add(
            	new Segment.Record(newSegment.getLength(), data.length));
            
            Storage storage =
            	Snapshot.storageFactory.getStorage(
        			newSegment.getStorageName());
            
            try
            {
            	storage.append(newSegment.getKey(), data);
            } finally
            {
            	storage.close();
            }

            if (!startLogIndexSegmentMap.containsKey(newSegment.getStartIndex()))
            {
                startLogIndexSegmentMap.put(
                	newSegment.getStartIndex(), newSegment);
            }

            totalSize += entrySize;
        }

        return newLastLogIndex;
    }

    public void truncatePrefix(long newFirstIndex)
    {
        if (newFirstIndex <= getFirstLogIndex())
        {
            return;
        }
        
        long oldFirstIndex = getFirstLogIndex();
        while (!startLogIndexSegmentMap.isEmpty())
        {
            Segment segment =
            	startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite())
            {
                break;
            }

            if (newFirstIndex > segment.getEndIndex())
            {
            	Storage storage =
            		Snapshot.storageFactory.getStorage(
            			segment.getStorageName());

            	try
            	{
            		storage.delete(segment.getKey());

            		totalSize -= segment.getLength();
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                    
                    segment.setLength(0);
            	} finally
            	{
            		storage.close();
            	}
            } else
            {
                break;
            }
        }
        
        long newActualFirstIndex;
        
        if (startLogIndexSegmentMap.size() == 0)
        {
            newActualFirstIndex = newFirstIndex;
        } else
        {
            newActualFirstIndex = startLogIndexSegmentMap.firstKey();
        }

        updateMetaData(null, null, newActualFirstIndex);

        LOG.info("Truncating log from old first index {} to new first index {}",
            oldFirstIndex, newActualFirstIndex);
    }

    public void truncateSuffix(long newEndIndex)
    {
        if (newEndIndex >= getLastLogIndex())
        {
            return;
        }

        LOG.info("Truncating log from old end index {} to new end index {}",
            getLastLogIndex(), newEndIndex);
        while (!startLogIndexSegmentMap.isEmpty())
        {
            Segment segment = startLogIndexSegmentMap.lastEntry().getValue();
            
            if (newEndIndex == segment.getEndIndex())
            {
                break;
            } else
            if (newEndIndex < segment.getStartIndex())
            {
            	Storage storage =
            		Snapshot.storageFactory.getStorage(
            			segment.getStorageName());
        	
            	try
            	{
            		storage.delete(segment.getKey());

            		totalSize -= segment.getLength();
                    startLogIndexSegmentMap.remove(segment.getStartIndex());
                    
                    segment.setLength(0);
            	} finally
            	{
            		storage.close();
            	}
            } else
            if (newEndIndex < segment.getEndIndex())
            {
                int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                segment.setEndIndex(newEndIndex);
                
                int newFileSize = segment.getEntries().get(i).offset;
                totalSize -= (segment.getLength() - newFileSize);
                
                segment.setLength(newFileSize);
                segment.getEntries().removeAll(
                    segment.getEntries().subList(i, segment.getEntries().size()));
                
                Storage storage =
                	Snapshot.storageFactory.getStorage(
                		segment.getStorageName());

                try
                {
                	String newFileName = String.format("%020d-%020d",
                        segment.getStartIndex(), segment.getEndIndex());

                	byte[] data = storage.get(
                		segment.getKey(), 0, segment.getLength());

                	storage.put(newFileName, data);
                	
                	storage.delete(segment.getKey());
                	
                	segment.setKey(newFileName);
                } finally
                {
                	storage.close();
                }
            }
        }
    }

    public void loadSegmentData(Segment segment)
    {
        try
        {
            int offset = 0;
            int totalLength = segment.getLength();
        
            DataInputStream input = null;
            
            Storage storage =
            	Snapshot.storageFactory.getStorage(
            		segment.getStorageName());
            
            try
            {
            	input =
            		new DataInputStream(new ByteArrayInputStream(
            			storage.get(segment.getKey())));
            } finally
            {
            	storage.close();
            }
            
            while (offset < totalLength)
            {
                RaftMessage.LogEntry entry =
                	RaftFileUtils.readProtoFromFile(
            			input, totalLength, RaftMessage.LogEntry.class);
                if (entry == null)
                {
                    throw new RuntimeException("read segment log failed");
                }

                int position = totalLength - input.available();
                
                Segment.Record record =
                	new Segment.Record(offset, position - offset);

                segment.getEntries().add(record);

                offset = position;
            }

            totalSize += totalLength;
        } catch (Exception ex)
        {
            LOG.error("read segment meet exception, msg={}", ex.getMessage());
            throw new RuntimeException("file not found");
        }

        int entrySize = segment.getEntries().size();
        
        if (entrySize > 0)
        {
            segment.setStartIndex(
            	segment.getEntry(0).getIndex());
            segment.setEndIndex(
            	segment.getEntry(entrySize - 1).getIndex());
        }
    }

    public void readSegments()
    {
    	Storage storage =
        	Snapshot.storageFactory.getStorage(logDataDir);
    	
        try
        {
            List<String> fileNames = storage.getKeys();

            for(String fileName: fileNames)
            {
                String[] splitArray = fileName.split("-");                
                if (splitArray.length != 2)
                {
                    LOG.warn("segment filename[{}] is not valid", fileName);
                    continue;
                }

                Segment segment = new Segment();
                segment.setKey(fileName);
                
                if (splitArray[0].equals("open"))
                {
                    segment.setCanWrite(true);
                    segment.setStartIndex(Long.valueOf(splitArray[1]));
                    segment.setEndIndex(0);
                } else
                {
                    try
                    {
                        segment.setCanWrite(false);
                        segment.setStartIndex(Long.parseLong(splitArray[0]));
                        segment.setEndIndex(Long.parseLong(splitArray[1]));
                    } catch (NumberFormatException ex)
                    {
                        LOG.warn("segment filename[{}] is not valid", fileName);
                        continue;
                    }
                }

                segment.setLength(storage.get(segment.getKey()).length);

                startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
            }
        } finally
        {
        	storage.close();
        }
    }

    public RaftMessage.LogMetaData readMetaData()
    {
        String fileName = logDir + "-" + "metadata";
        String key = "metadata";
        
        Storage storage = Snapshot.storageFactory.getStorage(fileName);
        
        try
        {
	        byte[] data = storage.get(key);
	        if (data == null)
	        	return null;
	        
	        RaftMessage.LogMetaData metadata =
	        	RaftFileUtils.readProtoFromFile(
	                new DataInputStream(new ByteArrayInputStream(data)),
	                data.length, RaftMessage.LogMetaData.class);

	        return metadata;
        } finally
        {
        	storage.close();
        }
    }

    public void updateMetaData(
    	Long currentTerm, Integer votedFor, Long firstLogIndex)
    {
        RaftMessage.LogMetaData.Builder builder =
        	RaftMessage.LogMetaData.newBuilder(this.metaData);

        if (currentTerm != null)
        {
            builder.setCurrentTerm(currentTerm);
        }
        
        if (votedFor != null)
        {
            builder.setVotedFor(votedFor);
        }
        
        if (firstLogIndex != null)
        {
            builder.setFirstLogIndex(firstLogIndex);
        }
        
        this.metaData = builder.build();

        String fileName = logDir + "-" + "metadata";
        String key = "metadata";
        
        Storage storage = Snapshot.storageFactory.getStorage(fileName);
        
        try
        {
        	ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        	
            RaftFileUtils.writeProtoToFile(
            	new DataOutputStream(byteOutput), metaData);
            
            storage.put(key, byteOutput.toByteArray());
        } finally
        {
        	storage.close();
        }
    }

    public RaftMessage.LogMetaData getMetaData()
    {
        return metaData;
    }

    public long getTotalSize()
    {
        return totalSize;
    }
}
