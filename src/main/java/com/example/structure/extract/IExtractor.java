package com.example.structure.extract;

public interface IExtractor {
    void extract();

    default IExtractor execute() {
        extract();
        return this;
    }

    default IExtractor refresh() {
        return execute();
    }

    String getValue();

    default void click() {
    }

    default boolean isElementExists() {
        return true;
    }
}
