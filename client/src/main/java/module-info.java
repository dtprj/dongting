module dongting.client {
    requires static java.logging;
    requires static org.slf4j;
    requires com.carrotsearch.hppc;
    requires jctools.core;
    exports com.github.dtprj.dongting.buf;
    exports com.github.dtprj.dongting.common;
    exports com.github.dtprj.dongting.log;
    exports com.github.dtprj.dongting.net;
    exports com.github.dtprj.dongting.pb;
    exports com.github.dtprj.dongting.raft.client;
    exports com.github.dtprj.dongting.java8 to dongting.client.java11;
}