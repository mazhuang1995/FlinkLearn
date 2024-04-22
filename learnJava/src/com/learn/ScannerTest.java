package com.learn;

import java.util.Scanner;

public class ScannerTest {
    public static void main(String[] args){
        //创建Scanner对象
        Scanner scanner = new Scanner(System.in);

        System.out.println("请输入您的名字：");
        String name = scanner.nextLine();

        System.out.println("请输入您的年龄：");
        int age = scanner.nextInt();

        //输出用户输入的信息
        System.out.printf("hello,%s! you are %d years old.",name,age);

        //关闭Scanner
        scanner.close();

    }
}
