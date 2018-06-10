package com.cabbage.hadoop.common.rackaware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

public class CustomDNSToSwitchMapping implements DNSToSwitchMapping {

    public List<String> resolve(List<String> names) {
        List<String> list = new ArrayList<String>();
        if(names!=null&&names.size()>0){
            for (String name:names){
                //name 是客户端的ip （eg：192.168.100.101） 或者 hostname（eg:s101）

                //截取主机的编号
                //如果是hostname 类型
                if(name.startsWith("s")){
                    String  no = name.substring(1);
                    Integer ino = Integer.valueOf(no);

                    //获取到ip地址后
                    //判断该机器在哪个集群  例如 100-103 是集群1  其他是集群2
                    if(ino<103){
                        list.add("/rack1/"+ino);
                    }else {
                        list.add("/rack2/"+ino);
                    }
                }
            }
        }

        return list;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> list) {

    }
}
