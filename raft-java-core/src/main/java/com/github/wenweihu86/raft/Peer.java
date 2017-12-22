package com.github.wenweihu86.raft;

import java.util.ServiceLoader;

import com.github.wenweihu86.raft.proto.ProtocolFactory;
import com.github.wenweihu86.raft.proto.RaftMessage;
import com.github.wenweihu86.raft.proto.RemoteClient;
import com.github.wenweihu86.raft.service.RaftConsensusService;

/**
 * Created by wenweihu86 on 2017/5/5.
 */
public class Peer
{
    private RaftMessage.Server server;
    private RemoteClient client;
    private RaftConsensusService raftConsensusService;

    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    private volatile Boolean voteGranted;
    private volatile boolean isCatchUp;

    public static final ProtocolFactory protocolFactory;
    
    static {
    	ServiceLoader<ProtocolFactory> loader =
    		ServiceLoader.load(ProtocolFactory.class);
    	
    	protocolFactory = loader.iterator().next();
    }
    
    public Peer(RaftMessage.Server server)
    {
        this.server = server;
        this.client = protocolFactory.connect(server);

        raftConsensusService =
        	protocolFactory.getProxy(
        		client, RaftConsensusService.class);

        isCatchUp = false;
    }

    public RaftMessage.Server getServer() {
        return server;
    }

    public RemoteClient getRpcClient() {
        return client;
    }

    public RaftConsensusService getRaftConsensusService() {
        return raftConsensusService;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
}
