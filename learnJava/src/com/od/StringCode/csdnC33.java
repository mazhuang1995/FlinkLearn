package com.od.StringCode;

import java.util.Scanner;

public class csdnC33 {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        String s = scanner.nextLine();


        for (int i = 26; i >=1 ; i--) {
            String oldStr = i + (i > 9 ? "\\*" : "");
            String newStr = String.valueOf((char) ('a' + i - 1));
            s = s.replaceAll(oldStr, newStr);
        }

        System.out.println(s);

    }
}
