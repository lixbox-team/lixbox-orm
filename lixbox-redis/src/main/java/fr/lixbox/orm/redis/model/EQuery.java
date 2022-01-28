package fr.lixbox.orm.redis.model;

import redis.clients.jedis.search.Query;

/**
 * Query represents query parameters and filters to load results from the engine
 */
public class EQuery extends Query
{
    private String _queryString;
    
    
    public EQuery() 
    {
        super("*");
    }



    public EQuery(String queryString) 
    {
        super(queryString);
    }
    
    
    
    @Override
    public String toString()
    {
        return "Query [ " + this._queryString + "]";
    }
}