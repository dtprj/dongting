module dongting.server {
    requires dongting.client;
    requires dongting.client.java11;

    exports com.github.dtprj.dongting.raft.server;
}