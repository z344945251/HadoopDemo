package com.cabbage.hadoop.common.codec;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * 测试压缩与解压缩编解码器
 */
public class TestCode {

    private  String srcpath = "d:/codec/frxxz.txt";

    private String hdfs_url = "hdfs://node101:9000";

    private FileSystem fs;

    @Before
    public  void init(){
        try{
            Configuration conf = new Configuration();
            fs = FileSystem.get(new URI(hdfs_url),conf,"hadop");
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Deflate
     */
    @Test
    public void DeflateTest() throws Exception {

        FileInputStream fis = new FileInputStream(srcpath);

        Class clazz  = DefaultCodec.class;
        //创建一个编解码器
        DefaultCodec codec = new DefaultCodec();
        FileOutputStream fos = new FileOutputStream("d:/codec/deflatetest.deflate");
        //尝试构造一个FSDataOutputStream  直接将压缩后的内容输出到hdfs
        FSDataOutputStream fsos = fs.create(new Path(""));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis,cos,1024,true);
        System.err.println("over!");
    }

    /**
     * Deflate 压缩
     */
    @Test
    public void DeflateTest2() throws Exception {

        String codeClassname = "org.apache.hadoop.io.compress.DefaultCodec";
        Class codecClass = Class.forName(codeClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);

        FileInputStream fis = new FileInputStream(srcpath);
        FileOutputStream fos = new FileOutputStream("d:/codec/deflatetest.deflate");
        //尝试构造一个FSDataOutputStream  直接将压缩后的内容输出到hdfs
        FSDataOutputStream fsos = fs.create(new Path(""));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis,cos,1024,true);
        System.err.println("over!");
    }

    /**
     * 解压缩
     *
     */
    @Test
    public void DeflateDecompressTest() throws Exception {

        Configuration conf = new Configuration();
        CompressionCodecFactory f = new CompressionCodecFactory(conf);
        CompressionCodec codec = f.getCodec(new Path(""));
        CompressionInputStream cis =  codec.createInputStream(new FileInputStream(""));
        FileOutputStream fos = new FileOutputStream("");
        IOUtils.copyBytes(cis,fos,1024,true);
    }

    /**
     * 解压缩
     *
     */
    @Test
    public void DeflateDecompressTest2() throws Exception {

        Configuration conf = new Configuration();
        Class<DefaultCodec> codecclass = DefaultCodec.class;
        DefaultCodec codec = ReflectionUtils.newInstance(codecclass,conf);
        CompressionInputStream cis =  codec.createInputStream(new FileInputStream(""));
        FileOutputStream fos = new FileOutputStream("");
        IOUtils.copyBytes(cis,fos,1024,true);
    }



    //压缩解压缩通用方法
    //压缩
    public void compress(Class codecClass,String srcPath ,String destPath) throws Exception{

        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);

        FileInputStream fis = new FileInputStream(srcPath);
        FileOutputStream fos = new FileOutputStream(destPath);
        //尝试构造一个FSDataOutputStream  直接将压缩后的内容输出到hdfs
        //FSDataOutputStream fsos = fs.create(new Path(""));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis,cos,1024,true);
        System.err.println("over!");
    }

    //解压缩

    public void decompress(Class codecClass,String srcPath ,String destPath) throws Exception{

        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);

        //文件的扩展名需要和使用的压缩工具保持一致  codec.getDefaultExtension()可获取压缩格式的扩展名
        CompressionInputStream cis =  codec.createInputStream(new FileInputStream(srcPath));
        FileOutputStream fos = new FileOutputStream(destPath);
        IOUtils.copyBytes(cis,fos,1024,true);
    }



    //codec Pool
    public void codecPoolTest(Class codecClass,String srcPath ,String destPath) throws IOException {
        Compressor com = null;
        try {
            Configuration conf = new Configuration();
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);
            com =  CodecPool.getCompressor(codec);
            CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream(destPath),com);
            FileInputStream fis = new FileInputStream(srcPath);
            IOUtils.copyBytes(fis,cos,1024,true);
        }finally {
            CodecPool.returnCompressor(com);
        }
    }
}
