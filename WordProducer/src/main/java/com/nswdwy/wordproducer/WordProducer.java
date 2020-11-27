package com.nswdwy.wordproducer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-12 18:30
 */
public class WordProducer {
    private static final String[] words = new String[]{"January", "February", "March", "April",
            "May", "June", "July", "August", "September", "October", "November", "December"};

    public static void main(String[] args) throws IOException {
        BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream("d:/word.txt"));

        for (int i = 0; i < 100000000; i++) {
            fos.write((words[(int) (Math.random() * words.length)] + " ").getBytes());
            if (i % 10 == 0) {
                fos.write('\n');
            }
        }

        fos.close();
    }
}
