package com.example.structure.extract.ExtractorImp;

import com.example.structure.extract.IExtractor;

public class XpathExtractor implements IExtractor {
    @Override
    public void extract() {

    }

    @Override
    public IExtractor execute() {
        return IExtractor.super.execute();
    }

    @Override
    public IExtractor refresh() {
        return IExtractor.super.refresh();
    }

    @Override
    public String getValue() {
        return null;
    }

    @Override
    public void click() {
        IExtractor.super.click();
    }

    @Override
    public boolean isElementExists() {
        return IExtractor.super.isElementExists();
    }
}
