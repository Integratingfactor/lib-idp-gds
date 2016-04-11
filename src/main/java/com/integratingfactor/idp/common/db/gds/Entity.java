package com.integratingfactor.idp.common.db.gds;

import com.google.gcloud.datastore.Key;

public class Entity<T> {
    T value;

    com.google.gcloud.datastore.Entity gdsEntity;

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public Key getKey() {
        return gdsEntity.key();
    }

    public Key getParentKey() {
        return gdsEntity.key().parent();
    }

    public com.google.gcloud.datastore.Entity getGdsEntity() {
        return gdsEntity;
    }

    public void setGdsEntity(com.google.gcloud.datastore.Entity gdsEntity) {
        this.gdsEntity = gdsEntity;
    }
}
