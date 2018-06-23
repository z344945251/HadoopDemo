package com.cabbage.hadoop.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class HelloWorldServiceImpl implements HelloWorldService {


    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return 1;
    }

    public ProtocolSignature getProtocolSignature(String protocol,
                                                  long clientVersion,
                                                  int clientMethodsHash) throws IOException {
        try {
            return ProtocolSignature.getProtocolSignature(protocol,clientVersion);
        }catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public String sayHello(String msg) {
        System.out.println(msg);
        return  "hello : "+msg;
    }

    public String sayHello1(String msg) {
        System.out.println(msg+"1");
        return  "hello1 : "+msg;
    }
}
