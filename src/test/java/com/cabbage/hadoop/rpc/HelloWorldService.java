package com.cabbage.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface HelloWorldService extends VersionedProtocol {
    long versionID = 1;

    String sayHello(String msg);

    String sayHello1(String msg);
}
