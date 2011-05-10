package com.mindplex.util;

/**
 *
 * @author Abel Perez
 */
public final class Check
{
    private Check() {}
    
    public static <T> T forNull(T suspect) {
        if (suspect == null) {
            throw new NullPointerException();
        }
        return suspect;
    }

    public static void argument(boolean expression, String message) {
        if (! expression) {
            throw new IllegalArgumentException(String.valueOf(message));
        }
    }
}
