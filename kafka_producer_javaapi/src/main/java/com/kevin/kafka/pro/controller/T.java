package com.kevin.kafka.pro.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * com.tvt.kafka.pro.controller.T
 *
 * @author TX
 * @version [1.0.0, 2022/11/29]
 */
public class T {

    public static void main_bak1(String[] args) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 199; i++) {
            list.add(i);
        }
        System.out.println("size: " + list.size());
        System.out.println(list);

        int batchCount = 100;
        int fromIndex = 0;
        int endIndex = fromIndex + batchCount;
        while (fromIndex < list.size()) {
            endIndex = endIndex < list.size()? endIndex: list.size();
            List<Integer> subList = list.subList(fromIndex, endIndex);
            fromIndex += batchCount;
            endIndex = fromIndex + batchCount;
            System.out.println(subList);
        }
    }

    public static void main_bak(String[] args) {

        Map<Long, Map<Long, Integer>> mp1 = new HashMap<>();
        Map<Long, Integer> mp11 = new HashMap<>();
        mp11.put(11L, 111);
        mp1.put(1L, mp11);

        Object obj = mp1;

        Map<Long, Object> mp2 = (Map<Long, Object>) obj;

        System.out.println(mp2);
    }

}
