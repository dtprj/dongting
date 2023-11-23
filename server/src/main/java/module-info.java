module dongting.server {
    requires transitive dongting.client;

    exports com.github.dtprj.dongting.raft.server;
    exports com.github.dtprj.dongting.raft.store;
    exports com.github.dtprj.dongting.raft.sm;
}