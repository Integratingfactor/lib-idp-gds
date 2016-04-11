package com.integratingfactor.idp.common.db.gds;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.springframework.core.env.Environment;
import org.springframework.util.SerializationUtils;

import com.google.gcloud.datastore.Blob;
import com.google.gcloud.datastore.BlobValue;
import com.google.gcloud.datastore.Datastore;
import com.google.gcloud.datastore.DatastoreOptions;
import com.google.gcloud.datastore.Entity.Builder;
import com.google.gcloud.datastore.KeyFactory;
import com.google.gcloud.datastore.PathElement;
import com.google.gcloud.datastore.Query;
import com.google.gcloud.datastore.QueryResults;
import com.google.gcloud.datastore.StructuredQuery.PropertyFilter;
import com.integratingfactor.idp.common.db.exceptions.DbException;
import com.integratingfactor.idp.common.db.exceptions.GenericDbException;
import com.integratingfactor.idp.common.db.exceptions.NotFoundDbException;

public class GdsDaoService {

    private static Logger LOG = Logger.getLogger(GdsDaoService.class.getName());

    String serviceNameSpace = null;

    // static final String GdsDaoNameSpaceEnvKey =
    // "idp.service.db.keyspace.name";
    static final String GdsDaoNameSpaceEnvKey = "idpServiceDbKeyspaceName";

    ConcurrentHashMap<String, KeyFactory> factory = new ConcurrentHashMap<String, KeyFactory>();

    ConcurrentHashMap<String, Field[]> fields = new ConcurrentHashMap<String, Field[]>();

    ConcurrentHashMap<String, Map<Field, Method>> getters = new ConcurrentHashMap<String, Map<Field, Method>>();

    ConcurrentHashMap<String, Map<Field, Method>> setters = new ConcurrentHashMap<String, Map<Field, Method>>();

    ConcurrentHashMap<String, Class<? extends Object>> classes = new ConcurrentHashMap<String, Class<? extends Object>>();

    private Datastore datastore;

    synchronized protected Datastore gds() {
        if (datastore == null) {
            datastore = DatastoreOptions.builder().namespace(serviceNameSpace).build().service();
        }
        return datastore;
    }

    public GdsDaoService() {
        serviceNameSpace = System.getenv().get(GdsDaoNameSpaceEnvKey);
        assert (serviceNameSpace != null);
    }

    public GdsDaoService(Environment env) {
        serviceNameSpace = env.getProperty(GdsDaoNameSpaceEnvKey);
        assert (serviceNameSpace != null);
    }

    public <T> void registerDaoEntity(Class<T> type) throws DbException {
        // walk through the fields of the entity type to make sure we have
        // correct annotations
        int idCount = 0;
        Map<Field, Method> getters = new HashMap<Field, Method>();
        Map<Field, Method> setters = new HashMap<Field, Method>();
        for (Field field : type.getDeclaredFields()) {
            // register getter method for this field
            getters.put(field, findMethod(type, field, "get"));
            // register getter method for this field
            setters.put(field, findMethod(type, field, "set"));

            if (field.isAnnotationPresent(Parent.class)) {
                // skip any registration for parents
                continue;
            } else if (field.isAnnotationPresent(Id.class)) {
                idCount++;
            }
            if (!Serializable.class.isAssignableFrom(field.getType())) {
                LOG.warning("Unsupported entity field " + field.getName() + " of type: " + field.getType());
                throw new GenericDbException("unsupported field type: " + field.getType());
            }
        }
        // there should be exactly one @Id field in an entity type
        if (idCount != 1) {
            LOG.warning("incorrect number of ID count: " + idCount);
            throw new GenericDbException("incorrect number of ID");
        }

        // everything is good, register this entity type
        LOG.info("Registering entity type " + type.getName());
        // register a key for this entity type
        factory.put(type.getSimpleName(), gds().newKeyFactory().kind(type.getSimpleName()));
        // register entity's methods
        this.getters.put(type.getSimpleName(), getters);
        this.setters.put(type.getSimpleName(), setters);
        // register class itself
        classes.put(type.getSimpleName(), type);
        fields.put(type.getSimpleName(), type.getDeclaredFields());
    }

    public static <T> Method findMethod(Class<T> type, Field field, String action) throws DbException {
        for (Method method : type.getMethods()) {
            if ((method.getName().startsWith(action))
                    && (method.getName().length() == (field.getName().length() + 3))) {
                if (method.getName().toLowerCase().endsWith(field.getName().toLowerCase())) {
                    return method;
                }
            }
        }
        throw new GenericDbException("did not find " + action + " method for property " + field.getName()
                + " in entity type " + type.getSimpleName());
    }

    public <T> void delete(Key<T> key) throws DbException {
        try {
            gds().delete(toGdsKey(key));
        } catch (Exception e) {
            e.printStackTrace();
            throw new GenericDbException(e.getMessage());
        }
    }

    public <T> void deletePk(Key<T> key) throws DbException {
        try {
            Query<com.google.gcloud.datastore.Key> query = Query.keyQueryBuilder()
                    .filter(PropertyFilter.hasAncestor(toGdsKey(key))).build();
            QueryResults<com.google.gcloud.datastore.Key> result = gds().run(query);
            while (result.hasNext()) {
                gds().delete(result.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new GenericDbException(e.getMessage());
        }
    }

    @SuppressWarnings("rawtypes")
    private com.google.gcloud.datastore.Key toGdsKey(Key key) throws GenericDbException {
        KeyFactory keyF = factory.get(key.clazz.getSimpleName());
        if (keyF == null) {
            LOG.warning("attempt to read an unregistered entity: " + key.clazz.getSimpleName());
            throw new GenericDbException("entity not registered");
        }
        if (key.parent == null) {
            return keyF.newKey(key.key);
        }
        // build ancestor path for the entity
        try {
            Deque<PathElement> ancestors = new ArrayDeque<PathElement>();
            addAncestory(ancestors, (Key) key.parent.invoke(key.entity));
            com.google.gcloud.datastore.Key entityKey = null;
            if (!ancestors.isEmpty()) {
                entityKey = gds().newKeyFactory().ancestors(ancestors).kind(key.clazz.getSimpleName()).newKey(key.key);
            } else {
                entityKey = keyF.newKey(key.key);
            }
            return entityKey;
        } catch (Exception e) {
            LOG.warning("failed to create GDS key from: " + key.clazz.getSimpleName());
            throw new GenericDbException(e.getMessage());
        }
    }

    @SuppressWarnings("rawtypes")
    public <T> List<Entity<T>> readByAncestorKey(Key key, Class<T> type) throws DbException {
        List<Entity<T>> entities = new ArrayList<Entity<T>>();
        KeyFactory keyF = factory.get(key.clazz.getSimpleName());
        if (keyF == null) {
            LOG.warning("attempt to read an unregistered entity: " + key.clazz.getSimpleName());
            throw new GenericDbException("entity not registered");
        }
        Query<com.google.gcloud.datastore.Entity> query = Query.entityQueryBuilder().kind(type.getSimpleName())
                // .filter(PropertyFilter.hasAncestor(keyF.newKey(key.key)))
                .filter(PropertyFilter.hasAncestor(toGdsKey(key)))
                .build();
        QueryResults<com.google.gcloud.datastore.Entity> result = gds().run(query);
        while (result.hasNext()) {
            entities.add(getFromEntity(result.next(), type));
        }
        return entities;
    }

    public <T> Entity<T> readByEntityKey(Key<T> key) throws DbException {
        KeyFactory keyF = factory.get(key.clazz.getSimpleName());
        if (keyF == null) {
            LOG.warning("attempt to read an unregistered entity: " + key.clazz.getSimpleName());
            throw new GenericDbException("entity not registered");
        }
        return getFromEntity(gds().get(toGdsKey(key)), key.clazz);
    }

    @SuppressWarnings("unchecked")
    private <T> Entity<T> getFromEntity(com.google.gcloud.datastore.Entity gdsEntity, Class<T> type)
            throws DbException {
        if (gdsEntity == null) {
            throw new NotFoundDbException("not found");
        }
        Entity<T> entity = new Entity<T>();
        entity.setGdsEntity(gdsEntity);
        // now construct the class instance from entity field by field
        try {
            Object instance = classes.get(type.getSimpleName()).newInstance();
            for (Field field : fields.get(type.getSimpleName())) {
                Object value = null;
                if (field.isAnnotationPresent(Parent.class)) {
                    // // for parent, get value recursively
                    // value = getFromEntity(entity, parents.get(type));
                    continue;
                } else if (field.isAnnotationPresent(Id.class)) {
                    // id fields get copied as is
                    value = gdsEntity.key().nameOrId();
                } else if (gdsEntity.contains(field.getName())) {
                    // non id field is saved/retrieved as blob
                    value = SerializationUtils.deserialize(gdsEntity.getBlob(field.getName()).toByteArray());
                }
                // field.set(instance, value);
                setters.get(type.getSimpleName()).get(field).invoke(instance, value);
            }
            entity.setValue((T) instance);
            return entity;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new GenericDbException(e.getMessage());
        }
    }

    @SuppressWarnings("rawtypes")
    private <T> void addAncestory(Deque<PathElement> ancestors, Key pk)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        ancestors.push(PathElement.of(pk.clazz.getSimpleName(), pk.key));
        Class<? extends Object> pkType = classes.get(pk.clazz.getSimpleName());
        for (Field pkField : pkType.getDeclaredFields()) {
            if (pkField.isAnnotationPresent(Parent.class)) {
                Method getter = getters.get(pk.entity.getClass().getSimpleName()).get(pkField);
                // keep adding to ancestor path
                addAncestory(ancestors, (Key) getter.invoke(pk.entity));
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public <T> void save(T entity) throws DbException {
        KeyFactory keyF = factory.get(entity.getClass().getSimpleName());
        if (keyF == null) {
            LOG.warning("attempt to save an unregistered entity: " + entity.getClass());
            throw new GenericDbException("entity not registered");
        }
        try {
            // process fields of the entity to create save query
            String key = null;
            // Class<? extends Object> type =
            // classes.get(entity.getClass().getSimpleName());
            Map<String, BlobValue> props = new HashMap<String, BlobValue>();
            Deque<PathElement> ancestors = new ArrayDeque<PathElement>();
            for (Field field : entity.getClass().getDeclaredFields()) {
                Method getter = getters.get(entity.getClass().getSimpleName()).get(field);
                if (field.isAnnotationPresent(Id.class)) {
                    // take note of @IdpDaoId annotated key name for the entity
                    key = (String) getter.invoke(entity);
                } else if (field.isAnnotationPresent(Parent.class)) {
                    // build ancestor path for the entity by adding key to
                    // parent
                    addAncestory(ancestors, (Key) getter.invoke(entity));
                } else {
                    props.put(field.getName(),
                            BlobValue.builder(Blob.copyFrom(SerializationUtils.serialize(getter.invoke(entity))))
                                    .excludeFromIndexes(true).build());
                }
            }
            // build a new key with ancestory
            com.google.gcloud.datastore.Key entityKey = null;
            if (!ancestors.isEmpty()) {
                entityKey = gds().newKeyFactory().ancestors(ancestors).kind(entity.getClass().getSimpleName())
                        .newKey(key);
            } else {
                entityKey = keyF.newKey(key);
            }
            // build a GDS save query from key and properties of the entity
            Builder builder = com.google.gcloud.datastore.Entity.builder(entityKey);
            for (Entry<String, BlobValue> kv : props.entrySet()) {
                builder.set(kv.getKey(), kv.getValue());
            }
            gds().put(builder.build());
        } catch (Exception e) {
            e.printStackTrace();
            throw new GenericDbException(e.getMessage());
        }
    }

    public <T> void save(List<T> entities) throws DbException {
        for (T entity : entities) {
            save(entity);
        }
    }
}
