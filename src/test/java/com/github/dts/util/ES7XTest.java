package com.github.dts.util;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class ES7XTest {

    public static void main(String[] args) {
        test1();

        test2();
    }

    private static void test1() {
        LinkedList<TrimRequest> requests1 = new LinkedList<>();

        requests1.add(new StringES("3"));
        requests1.add(new StringES("1"));
        requests1.add(new StringES("2"));
        requests1.add(new StringES("2"));
        requests1.add(new StringES("2"));
        requests1.add(new StringES("2"));
        requests1.add(new StringES("2"));
        requests1.add(new StringES("1"));
        requests1.add(new StringES("3"));
        TrimRequest.trim(requests1);
        System.out.println("requests1 = " + requests1);
    }

    private static Map asMap(Object... objects) {
        Map map = new LinkedHashMap();
        for (int i = 0; i < objects.length; i += 2) {
            map.put(objects[i], objects[i + 1]);
        }
        return map;
    }

    private static void test2() {
        LinkedList<TrimRequest> requests1 = new LinkedList<>();

        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "1", asMap("k", "v"), false, 1));
        requests1.add(new ES7xConnection.ES7xDeleteRequest("123",
                "1"));
        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "1", asMap("k", "v"), true, 1));
        requests1.add(new ES7xConnection.ES7xDeleteRequest("123",
                "1"));
        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "1", asMap("k", "v"), false, 1));
        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "1", asMap("k", "v"), false, 1));
        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "1", asMap("k", "v"), false, 1));
        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "1", asMap("ka", "v2"), true, 1));
        requests1.add(new ES7xConnection.ES7xUpdateRequest("123",
                "2", asMap("k", "v2"), true, 1));
        TrimRequest.trim(requests1);
        System.out.println("requests1 = " + requests1);
    }

    static class StringES implements TrimRequest {
        private final String string;

        StringES(String string) {
            this.string = string;
        }

        @Override
        public boolean isOverlap(TrimRequest request) {
            if (request instanceof StringES) {
                return string.equals(((StringES) request).string);
            }
            return false;
        }

        @Override
        public String toString() {
            return string;
        }
    }
}
