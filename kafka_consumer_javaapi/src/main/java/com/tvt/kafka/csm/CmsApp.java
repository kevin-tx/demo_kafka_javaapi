package com.tvt.kafka.csm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author TX
 * @date 2021/1/12 19:39
 */
public class CmsApp {
    private static Logger logger = LoggerFactory.getLogger(CmsApp.class);
    public static void main(String[] args) {
        Receiver receiver = new Receiver();
//        new Thread(() -> {
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.exit(0);
//        }).start();
        receiver.start();
    }
}
