package com.integratingfactor.idp.common.db.gds;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.integratingfactor.idp.common.db.exceptions.DbException;

public class Key<T> {
    // String type;

    Class<T> clazz;

    String key;

    T entity;

    Field field;

    Method parent;

    protected Key() {

    }

    public static class Pair {
        String key;
        Field field;
        Method parent = null;
    }

    public static <T> Key<T> create(T entity, Class<T> type) {
        Key<T> daoKey = new Key<T>();
        Pair pair = findKey(entity);
        if (pair == null)
            return null;
        daoKey.clazz = type;
        daoKey.key = pair.key;
        daoKey.field = pair.field;
        daoKey.entity = entity;
        // daoKey.type = type.getSimpleName();
        daoKey.parent = pair.parent;
        return daoKey;
    }

    private static <T> Pair findKey(T entity) {
        Pair pair = new Pair();
        try {
            for (Field field : entity.getClass().getDeclaredFields()) {
                if (field.isAnnotationPresent(Id.class)) {
                    pair.field = field;
                    pair.key = (String) GdsDaoService.findMethod(entity.getClass(), field, "get").invoke(entity);
                } else if (field.isAnnotationPresent(Parent.class)) {
                    pair.parent = GdsDaoService.findMethod(entity.getClass(), field, "get");
                }
            }
        } catch (Exception | DbException e) {
        }
        return pair;
    }
}
