package com.cabbage.java.proxy.dynamicProxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class LiuDeHuaProxy {

    private LiuDeHua ldh = new LiuDeHua();

    public Person getProxy() {

        return (Person) Proxy.newProxyInstance(LiuDeHua.class.getClassLoader(),
                LiuDeHua.class.getInterfaces(),
                new InvocationHandler() {
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

                        if(method.getName().equals("sing")){
                            System.out.println("我是他的经纪人，要找他唱歌得先给十万块钱！！");
                            return  method.invoke(ldh,args);
                        }

                        if(method.getName().equals("dance")){
                            System.out.println("我是他的经纪人，要找他跳舞得先给二十万块钱！！");
                            return  method.invoke(ldh,args);
                        }
                        return null;
                    }
                }
        );
    }
}
