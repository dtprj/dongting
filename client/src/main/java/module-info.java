module dongting.client {
    requires static java.logging;
    requires static org.slf4j;
    requires jdk.unsupported;
    exports com.github.dtprj.dongting.buf;
    exports com.github.dtprj.dongting.common;
    exports com.github.dtprj.dongting.dtkv;
    exports com.github.dtprj.dongting.log;
    exports com.github.dtprj.dongting.net;
    exports com.github.dtprj.dongting.codec;
    exports com.github.dtprj.dongting.raft;
    exports com.github.dtprj.dongting.queue to dongting.server;
}