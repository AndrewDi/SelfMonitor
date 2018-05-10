package org.selfmonitor.collector;

public class ComponentControl {
    public static void main(String argv[]) throws Exception {
        CollectorServer collectorServer=new CollectorServer();
        collectorServer.run();
        return;
    }
}