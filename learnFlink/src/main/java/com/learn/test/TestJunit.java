package com.learn.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestJunit {

    @Before
    public void test() {
        System.out.println("每次运行之前都要运行的内容");
    }

    @Test
    public void test1() {
        System.out.println("测试输出");
    }


    @After
    public void test2() {
        System.out.println("每次运行之后都要运行的内容");
    }
}
