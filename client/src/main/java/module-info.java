module dongting.client {
    requires static java.logging;
    requires static org.slf4j;
    exports com.github.dtprj.dongting.buf;
    exports com.github.dtprj.dongting.common;
    exports com.github.dtprj.dongting.log;
    exports com.github.dtprj.dongting.net;
    exports com.github.dtprj.dongting.pb;
    exports com.github.dtprj.dongting.queue;
    exports com.github.dtprj.dongting.raft.client;
    exports com.github.dtprj.dongting.java8;
    opens com.github.dtprj.dongting.common to dongting.client.java11;
}