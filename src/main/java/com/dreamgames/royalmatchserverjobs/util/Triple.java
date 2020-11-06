package com.dreamgames.royalmatchserverjobs.util;

import lombok.ToString;

@ToString
public class Triple<X, Y, Z> {
    public static final Triple<Boolean, Object, Object> TRUE = new Triple<>(true, null, null);
    public static final Triple<Boolean, Object, Object> FALSE = new Triple<>(false, null, null);
    public static final Triple EMPTY = null;

    public final X first;
    public final Y second;
    public final Z third;

    public Triple(X first, Y second, Z third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}