/*******************************************************************************
 *    
 *                           FRAMEWORK Lixbox
 *                          ==================
 *      
 *   Copyrigth - LIXTEC - Tous droits reserves.
 *   
 *   Le contenu de ce fichier est la propriete de la societe Lixtec.
 *   
 *   Toute utilisation de ce fichier et des informations, sous n'importe quelle
 *   forme necessite un accord ecrit explicite des auteurs
 *   
 *   @AUTHOR Ludovic TERRAL
 *
 ********************************************************************************/
package fr.lixbox.jee.redis.util;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import fr.lixbox.common.exceptions.BusinessException;
import fr.lixbox.common.guid.GuidGenerator;
import fr.lixbox.common.util.CollectionUtil;
import fr.lixbox.common.util.ExceptionUtil;
import fr.lixbox.common.util.StringUtil;
import fr.lixbox.io.json.JsonUtil;
import fr.lixbox.jee.entity.model.Dao;
import fr.lixbox.jee.entity.model.OptimisticDao;
import fr.lixbox.jee.redis.model.RedisSearchDao;
import io.redisearch.Client;
import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client.IndexOptions;
import redis.clients.jedis.Jedis;

/**
 * Cette classe interface l'univers redis avec l'univers POJO.
 * 
 * @author ludovic.terral
 */
public class ExtendRedisClient implements Serializable
{    
    private static final long serialVersionUID = -3968936170594429132L;
    private static final Log LOG = LogFactory.getLog(ExtendRedisClient.class);
    
    private Jedis redisClient;
    private Map<String, Client> searchClients;
    private String host;    
    private int port;
    
    
    /**
     * Ce constructeur sert à l'initialisation de l'acces a la base.
     * L'uri est de la forme "mongodb://user1:pwd1@host1/?authSource=db1&ssl=true"
     * 
     * @param uri
     */
    public ExtendRedisClient(String host, int port) 
    {        
        this.host = host;
        this.port = port;
        this.searchClients = new HashMap<>();
    }
    
    
    
    public boolean open()
    {
        //ouverture du client redis
        boolean isOpen = false;
        if (redisClient==null || !redisClient.isConnected())
        {
            redisClient = new Jedis(host, port);
            redisClient.connect();
        }
        isOpen = "pong".equalsIgnoreCase(redisClient.ping());
        
        
        return isOpen;
    }
    
    
    
    public void close()
    {
        if (redisClient!=null)
        {
            redisClient.close();
        }
        if (searchClients.size()>0)
        {
            for (Client searchClient : searchClients.values())
            {
                try 
                {
                    searchClient.close();
                }
                catch (Exception e)
                {
                    //pas actif
                }
            }
            searchClients.clear();
        }
    }
    
    
    
    public List<String> getKeys(String pattern)
    {
        String internamPattern = StringUtil.isEmpty(pattern)?"*":pattern;
        redisClient.connect();
        List<String> result  = new ArrayList<>(redisClient.keys(internamPattern));
        redisClient.close();
        return result;
    }
    
    
    
    /**
     * Cette methode renvoie la valeur associée à une clé
     * @param key
     * 
     * @return null si pas de valeur.
     */
    public String get(String key)
    {
        String result = "";
        if (key!=null)
        {
            redisClient.connect();
            switch (redisClient.type(key))
            {
                case "string":
                    result = redisClient.get(key);
                    break;
                default:
                    LOG.error("UNSUPPORTED FORMAT "+redisClient.type(key));
            }
            redisClient.close();
        }
        return result;        
    }

    

    /**
     * Cette methode supprime une clé et sa valeur dans le cache.
     * @param key
     * 
     * @return true si la suppression est effective.
     */
    public boolean remove(String key)
    {
        boolean result = false;
        if (key!=null)
        {
            redisClient.connect();
            switch (redisClient.type(key))
            {
                case "string":
                    if (redisClient.del(key)>0)
                    {
                        result = true;
                    } 
                    break;
                default:
                    LOG.error("UNSUPPORTED FORMAT "+redisClient.type(key));
            }
            redisClient.close();
        }
        return result;
    }

    

    /**
     * Cette methode supprime les clés et leurs valeurs dans le cache.
     * @param keys
     * 
     * @return true si la suppression est effective.
     */
    public boolean remove(String... keys)
    {
        boolean result = false;
        if (keys!=null)
        {
            redisClient.connect();
            if (redisClient.del(keys)>0)
            {
                result = true;
            } 
            redisClient.close();
        }
        return result;
    }
      
    
    
    /**
     * Cette methode renvoie le nombre de clés qui correspondent à une pattern.
     * Si la pattern n'est pas renseigné le wildcar est utilisé.
     * @param pattern
     * 
     * return le nombre de clés.
     */
    public int size(String pattern)
    {
        String internamPattern = StringUtil.isEmpty(pattern)?"*":pattern;
        redisClient.connect();
        List<String> result  = new ArrayList<>(redisClient.keys(internamPattern));
        redisClient.close();        
        return result.size();
    }
    
    

    /**
     * Cette methode verifie la présence d'une clé.     
     * @param pattern
     * 
     * return true si la clé est présente.
     */
    public boolean containsKey(String pattern)
    {  
        boolean result;
        List<String> tmp  = getKeys(pattern);        
        result = !tmp.isEmpty();
        return result;
    }
        
    
    
    /**
     * Cette methode insère une clé et sa valeur dans le cache.
     * @param key
     * @param value
     * 
     * @return true si l'enregistrement est effectif.
     */
    public boolean put(String key, String value)
    {
        boolean result=false;
        if (!StringUtil.isEmpty(key))
        {
            redisClient.connect();
            result = !StringUtil.isEmpty(redisClient.set(key,value));
            redisClient.expire(key, 60*60*24*15);
            redisClient.close();  
        }
        return result;
    }
    

    
    /**
     * Cette methode efface l'ensemble des données du cache.
     * 
     * @return true si le nettoyage est ok.
     */
    public boolean clear()
    {
        boolean result;
        redisClient.connect();
        result = redisClient.flushAll().contains("OK");
        redisClient.close();
        
        if (searchClients.size()>0)
        {
            for (Client searchClient : searchClients.values())
            {
                try 
                {
                    result &= searchClient.dropIndex();
                    searchClient.close();
                }
                catch (Exception e)
                {
                    //pas actif
                }
            }
            searchClients.clear();
        }
        return result;
    }
    

    
    
    /**
     * Cette methode enregistre les associations clé valeur dans le cache.
     * 
     * @param values
     * 
     * @return true si l'écriture est ok
     */
    public boolean put(Map<String,String> values)
    {
        boolean result;
        redisClient.connect();
        List<String> tmp = new ArrayList<>();        
        for (Entry<String, String> entry : values.entrySet())
        {
            tmp.add(entry.getKey());
            tmp.add(entry.getValue());
        }                
        result = redisClient.mset(tmp.toArray(new String[0])).contains("OK");
        redisClient.close();   
        return result;
    }

    
    
    /**
     * Cette methode récupère les valeurs associées à la liste des clés
     * fournie en paramètres
     * 
     * @param keys
     * 
     * @return la liste des valeurs
     */
    public Map<String, String> get(String... keys)
    {
        Map<String,String> result = new HashMap<>();
        redisClient.connect();
        List<String> values = redisClient.mget(keys);
        for (int ix=0; ix<keys.length; ix++)
        {
            result.put(keys[ix], values.get(ix));
        }                
        redisClient.close();
        return result;
    }
    
    
    
    public Object getTypedFromKey(String key)
    {
        Object result = null;        
        if (!StringUtil.isEmpty(key))
        {            
            String value = get(key);
            result = JsonUtil.transformJsonToObject(value, getTypeReferenceFromKey(key));
        } 
        return result;
    }
    
    

    @SuppressWarnings("unchecked")
    public <T extends Dao> List<T> getTypedFromKeys(List<String> keys)
    {
        List<T> result = new ArrayList<>();
        if (keys!=null && !keys.isEmpty())
        {            
            Map<String, String> convertMap = get(keys.toArray(new String[0]));
            for (Entry<String, String> entry : convertMap.entrySet())
            {
                if (StringUtil.isNotEmpty(entry.getValue()))
                {
                    result.add((T) JsonUtil.transformJsonToObject(entry.getValue(), getTypeReferenceFromKey(entry.getKey())));
                }
            }
        } 
        return result;
    }
    
    
    
    public <T extends RedisSearchDao> T merge(T object)
    {
        open();
        object = mergeNoManaged(object);
        close();
        return object;
    }
    
    
    
    public <T extends RedisSearchDao> List<T> merge(List<T> objects)
    {
        if (CollectionUtil.isEmpty(objects))
        {
            return objects;
        }
        open();
        for (T object : objects)
        {
            object = mergeNoManaged(object);
        }
        return objects;
    }
    
    
    
    public <T extends RedisSearchDao> void remove(Class<T> entityClass, String id) throws BusinessException
    {
        open();
        try
        {
            T tmp = entityClass.newInstance();
            tmp.setOid(id);
            Client searchClient = searchClients.get(entityClass.getSimpleName());
            redisClient.del(tmp.getKey());
            searchClient.deleteDocument(id);
        }
        catch(Exception e) 
        {
            ExceptionUtil.traiterException(e, "Impossible de supprimer l'objet", false);
        }
        close();
    }  
    
    
    
    @SuppressWarnings("unchecked")
    public <T extends RedisSearchDao> T findById(Class<T> entityClass, String id) 
        throws BusinessException
    {
        T result = null;
        try 
        {
            T tmp = entityClass.newInstance();
            open();
            tmp.setOid(id);
            String json = redisClient.get(tmp.getKey());
            result = (T) JsonUtil.transformJsonToObject(json, getTypeReferenceFromClass(entityClass));
        }
        catch (Exception e)
        {
            ExceptionUtil.traiterException(e, "Impossible de charger la classe", true);
        }
        close();
        if (result == null)
        {
            LOG.error("No entity find with id "+id);
            throw new BusinessException("No entity find with id "+id);
        }
        return result;
    }



    public <T extends RedisSearchDao> List<T> findByExpression(Class<T> entityClass, String expression) 
        throws BusinessException
    {
        List<T> result = new ArrayList<>();
        
        Client searchClient = getSearchClientByClass(entityClass);
        open();
        
        Query q = new Query(expression);
        SearchResult res = searchClient.search(q);
        
        if (res.totalResults>0)
        {
            List<String> keys = new ArrayList<>();
            for (Document doc : res.docs) 
            {
                keys.add((String) doc.get("key"));
            }
            result = getTypedFromKeys(keys);
        }
        close();
        if (CollectionUtil.isEmpty(result))
        {
            LOG.error("No entity find with expression "+expression);
            throw new BusinessException("No entity find with expression "+expression);
        }
        return result;
    }
    
    
    
    private <T extends RedisSearchDao> Client getSearchClient(T object)
    {
        //ouverture du client redis
        if (!searchClients.containsKey(object.getClass().getSimpleName()))
        {            
            searchClients.put(object.getClass().getSimpleName() , new io.redisearch.client.Client(object.getClass().getSimpleName(), host, port));
            try
            {
                searchClients.get(object.getClass().getSimpleName()).createIndex(object.getIndexSchema(), IndexOptions.defaultOptions());
            }
            catch (Exception e)
            {
                //existe peut être
            }
        }
        return searchClients.get(object.getClass().getSimpleName());
    }
    

    
    private <T extends RedisSearchDao> Client getSearchClientByClass(Class<T> entityClass)
    {
      //ouverture du client redis
        if (!searchClients.containsKey(entityClass.getSimpleName()))
        {            
            searchClients.put(entityClass.getSimpleName() , new io.redisearch.client.Client(entityClass.getSimpleName(), host, port));
            try
            {
                searchClients.get(entityClass.getSimpleName()).createIndex(entityClass.newInstance().getIndexSchema(), IndexOptions.defaultOptions());
            }
            catch (Exception e)
            {
                //existe peut être
            }
        }
        return searchClients.get(entityClass.getSimpleName());
    }



    private <T extends RedisSearchDao> TypeReference<T> getTypeReferenceFromClass(Class<T> classz)
    {
        return new TypeReference<T>(){
            @Override
            public Type getType() {
                Type type = classz;
                return type;
            }
        };
    }



    private <T> TypeReference<?> getTypeReferenceFromKey(String key)
    {
        String cleanKey = key.substring(key.indexOf(':', key.indexOf(':')+1)+1,key.indexOf(':', key.indexOf(':', key.indexOf(':')+1)+1));
        return new TypeReference<T>(){
            @Override
            public Type getType() {
                Type type = null;
                try
                {
                    type = Class.forName(cleanKey);
                }
                catch (ClassNotFoundException e)
                {
                    LOG.fatal(e);
                }
                return type;
            }
        };
    }
    
    
    
    private <T extends RedisSearchDao> T mergeNoManaged(T object)
    {
        Client searchClient = getSearchClient(object);
        if (object instanceof OptimisticDao)
        {
            ((OptimisticDao)object).setVersion(Calendar.getInstance());
        }
        if (StringUtil.isEmpty(object.getOid()))
        {
            object.setOid(GuidGenerator.getGUID(object));
            String json = JsonUtil.transformObjectToJson(object, false);
            redisClient.set(object.getKey(), json);
            Map<String, Object> indexField = new HashMap<>(object.getIndexFieldValues());
            indexField.put("oid", object.getOid());
            indexField.put("key", object.getKey());
            searchClient.addDocument(object.getOid(), indexField);
        }
        else
        {
            String json = JsonUtil.transformObjectToJson(object, false);
            redisClient.set(object.getKey(), json);
            Map<String, Object> indexField = new HashMap<>(object.getIndexFieldValues());
            indexField.put("oid", object.getOid());
            indexField.put("key", object.getKey());
            searchClient.replaceDocument(object.getOid(), 1, indexField);
        }
        return object;
    }
}