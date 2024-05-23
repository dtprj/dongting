module dongting.server {
    requires transitive dongting.client;
    requires jdk.jfr;

    exports com.github.dtprj.dongting.fiber;
    exports com.github.dtprj.dongting.raft.server;
    exports com.github.dtprj.dongting.raft.store;
    exports com.github.dtprj.dongting.raft.sm;
    exports com.github.dtprj.dongting.dtkv.server;
}