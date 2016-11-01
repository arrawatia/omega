package io.omega;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sumit on 11/1/16.
 */
public class Foo {

        private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");
    public static void main(String... args){
        Matcher matcher = HOST_PORT_PATTERN.matcher("PLAINTEXT://0.0.0.0:9090");
        if (matcher.matches())
            System.out.println(matcher.group(0));
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
    }
}
