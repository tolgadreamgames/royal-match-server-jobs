package com.dreamgames.royalmatchserverjobs.util;

import lombok.ToString;

@ToString
public class Tuple<X, Y> {
    public static final Tuple<Boolean, Object> TRUE = new Tuple<>(true, null);
    public static final Tuple<Boolean, Object> FALSE = new Tuple<>(false, null);
    public static final Tuple EMPTY = null;

    public final X left;
    public final Y right;

    public Tuple(X left, Y right) {
        this.left = left;
        this.right = right;
    }
}