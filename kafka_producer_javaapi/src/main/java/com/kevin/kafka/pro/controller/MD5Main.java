package com.kevin.kafka.pro.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * com.tvt.kafka.pro.controller.MD5Main
 *
 * @author TX
 * @version [1.0.0, 2022/10/15]
 */
public class MD5Main {

    // IPC SN sql
    public static void main(String[] args) throws IOException {
        for (Integer i = 1; i < 65; i++) {
            String chlName = "IPC" + String.format("%02d", i);
            String chlSn = DigestUtils.md5Hex(chlName).toUpperCase();
            int chlIndex = i;
            String devSn = "7645418863A5EE9A17C8463933A8119B";
            if(i>32){
                chlIndex = i-32;
                devSn = "0CD8032C026AE8AC5A027472C9BD73FB";
            }
            int verIdIndex = (i-1)%13+1;
            int chlId = 80000 + i;

            String sql = "INSERT INTO apply_device.resource_channel (sn,chl_index,model,version_id,version,ip,status,capability,online_time,offline_time,id,chl_name,chl_sn,mac) VALUES\n"
                + "('"+devSn+"',"+chlIndex+",'C2 4MP 5.2','verid-ipc"+verIdIndex+"','5.2.0.000"+String.format("%02d", verIdIndex)+"B220921',NULL,1,'','2022-10-15 12:15:01',NULL,"+chlId+",'chl-"+i+"','"+chlSn+"','70:00:00:00:00:"+String.format("%02d", i)+"');";
            System.out.println(sql);
        }
    }


    // IPC SN md5
    public static void main_sn(String[] args) throws IOException {
        for (Integer i = 1; i < 65; i++) {
            String chlName = "IPC" + String.format("%02d", i);
            System.out.println(chlName + ":" + DigestUtils.md5Hex(chlName).toUpperCase());
        }
    }



    // IPC pkg md5
    public static void main_pkg(String[] args) throws IOException {
        for (int i = 1; i < 15; i++) {
            String chlName = "ipc" + i;
            System.out.println(chlName + ":" + DigestUtils.md5Hex(chlName));
        }
    }

    public static void main_bak(String[] args) throws IOException {
        File file = new File("D:\\云升级测试文件\\模拟包\\");
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for (File file1 : files) {
                if(!file1.isDirectory()){
                    String md5FromOSS = DigestUtils.md5Hex(new FileInputStream(file1.getAbsoluteFile()));
                    System.out.println(file1.getName() + ":" + md5FromOSS);
                }
            }
        }
    }
}
