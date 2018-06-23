package com.cabbage.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.net.InetSocketAddress;

public class MyClient {
    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();

            HelloWorldService proxy = RPC.getProxy(HelloWorldService.class,
                    HelloWorldService.versionID,
                    new InetSocketAddress("localhost",8888),
                    conf);
            String result = proxy.sayHello1("world");

            System.out.println(result);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
