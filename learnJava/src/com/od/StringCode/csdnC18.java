package com.od.StringCode;

import java.util.Scanner;

public class csdnC18 {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        String str = scanner.nextLine();

        int countX = 0;
        int countY = 0;

        int countC = 0;

        for (int i = 0; i < str.length(); i++) {
            if(str.charAt(i) == 'X'){
                ++countX;
            }
            else{
                ++countY;
            }

            if(countX == countY){
                countC++;
            }


        }

        System.out.println(countC);
    }
}
