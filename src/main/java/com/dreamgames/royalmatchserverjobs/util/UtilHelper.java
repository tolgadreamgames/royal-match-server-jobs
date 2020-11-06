package com.dreamgames.royalmatchserverjobs.util;

public class UtilHelper {
    private final static long YEAR_2100 = 4102448400000L;

    public static double wrapScoreWithDate(int score, long date) {
        return Double.valueOf(score + "." + (YEAR_2100 - date));
    }
}
