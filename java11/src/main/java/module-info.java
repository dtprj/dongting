module dongting.java11 {
    requires static java.logging;
    requires static org.slf4j;
    requires jdk.unsupported;
    requires transitive dongting.client;
}