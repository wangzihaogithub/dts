package com.github.dts.util;

import java.util.function.Supplier;

public class LazyMetaDataRepository implements MetaDataRepository {
    private final Supplier<MetaDataRepository> supplier;
    private volatile MetaDataRepository repository;

    public LazyMetaDataRepository(Supplier<MetaDataRepository> supplier) {
        this.supplier = supplier;
    }

    public MetaDataRepository getRepository() {
        if (repository == null) {
            synchronized (supplier) {
                if (repository == null) {
                    repository = supplier.get();
                }
            }
        }
        return repository;
    }

    @Override
    public <T> T getCursor() {
        MetaDataRepository repository = getRepository();
        if (repository == null) {
            return null;
        }
        return repository.getCursor();
    }

    @Override
    public void setCursor(Object cursor) {
        MetaDataRepository repository = getRepository();
        if (repository == null) {
            return;
        }
        repository.setCursor(cursor);
    }

    @Override
    public String name() {
        MetaDataRepository repository = getRepository();
        String name = repository == null ? "null" : repository.name();
        return "Lazy(" + name + ")";
    }
}
