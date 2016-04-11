package com.integratingfactor.idp.common.db.gds;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.integratingfactor.idp.common.db.exceptions.DbException;
import com.integratingfactor.idp.common.db.exceptions.NotFoundDbException;
import com.integratingfactor.idp.common.db.gds.GdsDaoService;

@ContextConfiguration(classes = { GdsDaoServiceTestConfig.class })
public class GdsDaoServiceTest extends AbstractTestNGSpringContextTests {

    @Autowired
    GdsDaoService dao;

    public static class Model {
        String key;

        String another;

        String something;

        String value;
    }

    public static final String TestKey = "pk";

    public static final String TestAnother = "ck1-";

    public static final String TestSomething = "ck2-";

    public static final String TestValue = "value-";

    public static Model testModel(String suffix) {
        Model model = new Model();
        model.key = TestKey;
        model.another = TestAnother + suffix;
        model.something = TestSomething + suffix;
        model.value = TestValue + suffix;
        return model;
    }

    @AfterClass
    public void cleanup() throws DbException {
        dao.deletePk(TestDaoValueBySomethingByAnotherByKeyUtil.toPk(testModel("")));
    }

    @Test
    public void testGdsDaoServiceWriteReadByEntityKeyDeleteByEntityKey() throws DbException {
        Model wModel = testModel("first");
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKeyPk.class);
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKeyCk.class);
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKey.class);
        dao.save(TestDaoValueBySomethingByAnotherByKeyUtil.toEntity(wModel));
        Model rModel = TestDaoValueBySomethingByAnotherByKeyUtil
                .toModel(dao.readByEntityKey(TestDaoValueBySomethingByAnotherByKeyUtil.toKey(testModel("first"))));
        Assert.assertEquals(rModel.value, wModel.value);

        dao.delete(TestDaoValueBySomethingByAnotherByKeyUtil.toKey(rModel));
        try {
            dao.readByEntityKey(TestDaoValueBySomethingByAnotherByKeyUtil.toKey(testModel("first")));
            Assert.fail("data still exists after calling delete");
        } catch (NotFoundDbException e) {
            System.out.println("Success: " + e.getError());
        }

    }

    @Test
    public void testGdsDaoServiceWriteReadByCkDeleteByCk() throws DbException {
        Model wModel = testModel("first");
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKeyPk.class);
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKeyCk.class);
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKey.class);
        dao.save(TestDaoValueBySomethingByAnotherByKeyUtil.toEntity(wModel));
        // write one more variation for clustering
        dao.save(TestDaoValueBySomethingByAnotherByKeyUtil.toEntity(testModel("second")));
        List<Model> rModels = TestDaoValueBySomethingByAnotherByKeyUtil
                .toModel(dao.readByAncestorKey(TestDaoValueBySomethingByAnotherByKeyUtil.toCk(testModel("first")),
                        TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKey.class));
        Assert.assertEquals(rModels.get(0).value, wModel.value);

        dao.deletePk(TestDaoValueBySomethingByAnotherByKeyUtil.toCk(testModel("first")));
        try {
            dao.readByEntityKey(TestDaoValueBySomethingByAnotherByKeyUtil.toKey(testModel("first")));
            Assert.fail("data still exists after calling delete");
        } catch (NotFoundDbException e) {
            System.out.println("Success: " + e.getError());
        }
        // make sure that second entity in cluster is still there
        Model rModel = TestDaoValueBySomethingByAnotherByKeyUtil
                .toModel(dao.readByEntityKey(TestDaoValueBySomethingByAnotherByKeyUtil.toKey(testModel("second"))));
        Assert.assertEquals(rModel.value, testModel("second").value);

    }

    @Test
    public void testGdsDaoServiceWriteReadByPkDeleteByPk() throws DbException {
        Model wModel = testModel("first");
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKeyPk.class);
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKeyCk.class);
        dao.registerDaoEntity(TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKey.class);
        dao.save(TestDaoValueBySomethingByAnotherByKeyUtil.toEntity(wModel));
        // write one more variation for clustering
        dao.save(TestDaoValueBySomethingByAnotherByKeyUtil.toEntity(testModel("second")));
        List<Model> rModels = TestDaoValueBySomethingByAnotherByKeyUtil
                .toModel(dao.readByAncestorKey(TestDaoValueBySomethingByAnotherByKeyUtil.toPk(testModel("first")),
                        TestDaoValueBySomethingByAnotherByKeyUtil.TestDaoValueBySomethingByAnotherByKey.class));
        Assert.assertEquals(rModels.get(0).value, wModel.value);

        // delete by PK, and that should also delete entity for second cluster
        dao.deletePk(TestDaoValueBySomethingByAnotherByKeyUtil.toPk(testModel("first")));
        try {
            dao.readByEntityKey(TestDaoValueBySomethingByAnotherByKeyUtil.toKey(testModel("second")));
            Assert.fail("data still exists after calling delete");
        } catch (NotFoundDbException e) {
            System.out.println("Success: " + e.getError());
        }

    }
}
