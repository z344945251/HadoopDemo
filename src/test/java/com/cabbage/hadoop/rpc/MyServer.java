package com.cabbage.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class MyServer {


    public static void main(String[] args) {
        Server server;
        try {
            Configuration conf = new Configuration();
            server = new RPC.Builder(conf).setProtocol(HelloWorldService.class)
                    .setInstance(new HelloWorldServiceImpl())
                    .setBindAddress("localhost")
                    .setNumHandlers(2)
                    .setPort(8888).build();
            server.start();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
