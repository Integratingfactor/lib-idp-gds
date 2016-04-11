package com.integratingfactor.idp.common.db.gds;

import java.util.ArrayList;
import java.util.List;

import com.integratingfactor.idp.common.db.gds.Entity;
import com.integratingfactor.idp.common.db.gds.Id;
import com.integratingfactor.idp.common.db.gds.Key;
import com.integratingfactor.idp.common.db.gds.Parent;
import com.integratingfactor.idp.common.db.gds.GdsDaoServiceTest.Model;

/**
 * a Test data model for GDS DAO service to make sure it can handle composite
 * keys
 * 
 * <pre>
 * TABLE: value_by_something_by_another_by_key
 * primary key: key
 * clustering key 1: another
 * clustering key 2: something
 * column: value
 * </pre>
 * 
 * @author gnulib
 *
 */
public class TestDaoValueBySomethingByAnotherByKeyUtil {

    public static class TestDaoValueBySomethingByAnotherByKeyPk {
        @Id
        String key;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }

    public static class TestDaoValueBySomethingByAnotherByKeyCk {
        @Parent
        Key<TestDaoValueBySomethingByAnotherByKeyPk> pk;

        @Id
        String another;

        public Key<TestDaoValueBySomethingByAnotherByKeyPk> getPk() {
            return pk;
        }

        public void setPk(Key<TestDaoValueBySomethingByAnotherByKeyPk> pk) {
            this.pk = pk;
        }

        public String getAnother() {
            return another;
        }

        public void setAnother(String another) {
            this.another = another;
        }
    }

    public static class TestDaoValueBySomethingByAnotherByKey {
        @Parent
        Key<TestDaoValueBySomethingByAnotherByKeyCk> ck;

        @Id
        String something;

        String value;

        public Key<TestDaoValueBySomethingByAnotherByKeyCk> getCk() {
            return ck;
        }

        public void setCk(Key<TestDaoValueBySomethingByAnotherByKeyCk> ck) {
            this.ck = ck;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getSomething() {
            return something;
        }

        public void setSomething(String something) {
            this.something = something;
        }
    }

    public static TestDaoValueBySomethingByAnotherByKey toEntity(Model model) {
        TestDaoValueBySomethingByAnotherByKey entity = new TestDaoValueBySomethingByAnotherByKey();
        entity.value = model.value;
        entity.something = model.something;
        entity.ck = toCk(model);
        return entity;
    }

    public static Key<TestDaoValueBySomethingByAnotherByKey> toKey(Model model) {
        TestDaoValueBySomethingByAnotherByKey entity = new TestDaoValueBySomethingByAnotherByKey();
        entity.value = model.value;
        entity.something = model.something;
        entity.ck = toCk(model);
        return Key.create(entity, TestDaoValueBySomethingByAnotherByKey.class);
    }

    public static Model toModel(Entity<TestDaoValueBySomethingByAnotherByKey> entity) {
        Model model = new Model();
        model.key = entity.getParentKey().parent().name();
        model.another = entity.getParentKey().name();
        model.something = entity.getValue().something;
        model.value = entity.getValue().value;
        return model;
    }

    public static Key<TestDaoValueBySomethingByAnotherByKeyCk> toCk(Model model) {
        TestDaoValueBySomethingByAnotherByKeyCk ck = new TestDaoValueBySomethingByAnotherByKeyCk();
        ck.another = model.another;
        ck.pk = toPk(model);
        return Key.create(ck, TestDaoValueBySomethingByAnotherByKeyCk.class);
    }

    public static Key<TestDaoValueBySomethingByAnotherByKeyPk> toPk(Model model) {
        TestDaoValueBySomethingByAnotherByKeyPk pk = new TestDaoValueBySomethingByAnotherByKeyPk();
        pk.key = model.key;
        return Key.create(pk, TestDaoValueBySomethingByAnotherByKeyPk.class);
    }

    public static List<Model> toModel(List<Entity<TestDaoValueBySomethingByAnotherByKey>> entities) {
        List<Model> models = new ArrayList<Model>();
        for (Entity<TestDaoValueBySomethingByAnotherByKey> entity : entities) {
            models.add(toModel(entity));
        }
        return models;
    }

}
