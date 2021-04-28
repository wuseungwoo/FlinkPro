package com.seungwoo.utils;

import java.text.DecimalFormat;

public class MyBytes {
    public static String getNetFileSizeDescription(long size) {
        StringBuffer bytes = new StringBuffer();
        DecimalFormat format = new DecimalFormat("###.00");
        if (size >= 1024L * 1024L * 1024L * 1024L) {
            double i = (size / (1024.0 * 1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("TB");
        } else if (size >= 1024L * 1024L * 1024L) {
            double i = (size / (1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("GB");
        } else if (size >= 1024L * 1024L) {
            double i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MB");
        } else if (size >= 1024L) {
            double i = (size / (1024.0));
            bytes.append(format.format(i)).append("KB");
        } else if (size < 1024L) {
            bytes.append(size+"B");
        }
        return bytes.toString();
    }
}
