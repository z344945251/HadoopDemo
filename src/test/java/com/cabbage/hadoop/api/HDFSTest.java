package com.cabbage.hadoop.api;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;

public class HDFSTest {
    static {
        //注册协议处理器工厂  让java程序能够识别hdfs协议
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public void test001(){
        String url = "hdfs://node101:9000/user/hadoop/test/test.txt";
        try {
            URL u = new URL(url);
            InputStream is = u.openConnection().getInputStream();
            byte[] buf = new byte[1024];
            FileOutputStream os = new FileOutputStream("d:/test.txt");
            int len = -1;
            while ((len = is.read(buf))!=-1){
                os.write(buf);
            }
            is.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public  static void  main(String[] args){
        HDFSTest th = new HDFSTest();
        th.test001();
    }
}
