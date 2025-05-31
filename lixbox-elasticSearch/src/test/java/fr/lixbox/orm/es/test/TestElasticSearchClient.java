/*******************************************************************************
 *    
 *                           APPLICATION DATAFLOW
 *                          =======================
 * MIT License
 * <p>
 * Copyright (c) 2024 Ludovic TERRAL
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *   @AUTHOR Ludovic TERRAL
 *
 ******************************************************************************/
package fr.lixbox.orm.es.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import fr.lixbox.common.util.StringUtil;
import fr.lixbox.orm.entity.model.PaginatedResult;
import fr.lixbox.orm.es.client.ElasticSearchClient;
import fr.lixbox.orm.es.model.DataflowContainer;
import fr.lixbox.orm.es.model.DataflowInfo;
import fr.lixbox.orm.es.model.EntryPoint;
import fr.lixbox.orm.es.model.enumeration.DataFlowContainerType;

/**
 * Cette classe teste la classe PersistenceService
 * 
 * @author ludovic.terral
 */
class TestElasticSearchClient
{
    // ----------- Attribut(s) -----------
    private static ElasticSearchClient elasticSearchClient;
    private static final String DEFAULT_BODY_CONTENT = "My tailor is rich";
    

    
    // ----------- Methode(s) -----------
    @BeforeAll
    public static void setup()
    {
    	elasticSearchClient = new ElasticSearchClient("localhost", 9200, "http");
    }
    
    
    
    @Test
    void test_indexDataflowInfo_ok()
    {
        //preparation
        String correlationId = "123e4567-"+ java.util.Calendar.getInstance().getTimeInMillis();
        String archiveId = "ARCHID-"+ java.util.Calendar.getInstance().getTimeInMillis();
       
        DataflowInfo dataflowInfo = new DataflowInfo();
        dataflowInfo.setCorrelationId(correlationId);
        dataflowInfo.setContainerVersion("1.0");
        dataflowInfo.setType(DataFlowContainerType.TRANSPORT);
        dataflowInfo.setFrom("source");
        dataflowInfo.setTo("destination");
        dataflowInfo.setTimestamp(new Date());
        dataflowInfo.setArchiveId(correlationId+"_"+archiveId);
        dataflowInfo.setEntryPoint(new EntryPoint());
        Map<String, String> metadatas = new HashMap<>();
        metadatas.put("route", "maroute1");
        dataflowInfo.setMetadatas(metadatas);
        
        //oracle
        try 
        {
            assertTrue(StringUtil.isNotEmpty(elasticSearchClient.merge( dataflowInfo).getOid()), "L'enregistrement du DataflowInfo a échoué dans ES" );
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
    }
    
    
    
    @Test
    void test_indexDataflowContainer_ok()
    {
        //preparation
        String correlationId = "123e4567-"+ java.util.Calendar.getInstance().getTimeInMillis();
        String archiveId = "ARCHID-"+ java.util.Calendar.getInstance().getTimeInMillis();
       
        DataflowContainer dataflowContainer = new DataflowContainer();
        dataflowContainer.setCorrelationId(correlationId);
        dataflowContainer.setContainerVersion("1.0");
        dataflowContainer.setType(DataFlowContainerType.TRANSPORT);
        dataflowContainer.setFrom("source");
        dataflowContainer.setTo("destination");
        dataflowContainer.setTimestamp(new Date());
        dataflowContainer.setArchiveId(correlationId+"_"+archiveId);
        dataflowContainer.setEntryPoint(new EntryPoint());
        Map<String, String> metadatas = new HashMap<>();
        metadatas.put("route", "maroute-"+java.util.Calendar.getInstance().getTimeInMillis());
        dataflowContainer.setMetadatas(metadatas);
        dataflowContainer.setBody("123456");
        
        //oracle
        try 
        {
            assertTrue(StringUtil.isNotEmpty(elasticSearchClient.merge(dataflowContainer).getOid()), "L'enregistrement du DataflowContainer a échoué dans ES" );
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
    }
    
    
    
    @Test
    void test_getDaoById_dataflowContainer_ok()
    {
        //preparation
        String correlationId = "123e4567-"+ java.util.Calendar.getInstance().getTimeInMillis();
        String archiveId = "ARCHID-"+ java.util.Calendar.getInstance().getTimeInMillis();
       
        DataflowContainer dataflow = new DataflowContainer();
        dataflow.setCorrelationId(correlationId);
        dataflow.setContainerVersion("1.0");
        dataflow.setType(DataFlowContainerType.TRANSPORT);
        dataflow.setFrom("source");
        dataflow.setTo("destination");
        dataflow.setTimestamp(new Date());
        dataflow.setArchiveId(correlationId+"_"+archiveId);
        dataflow.setEntryPoint(new EntryPoint());
        Map<String, String> metadatas = new HashMap<>();
        metadatas.put("route", "maroute-"+java.util.Calendar.getInstance().getTimeInMillis());
        dataflow.setMetadatas(metadatas);
        dataflow.setBody(DEFAULT_BODY_CONTENT);
        try 
        {
            dataflow.getMetadatas().put("esId", elasticSearchClient.merge(dataflow).getOid());
            assertTrue(StringUtil.isNotEmpty(dataflow.getMetadatas().get("esId")), "L'enregistrement du DataflowInfoa échoué dans ES" );
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
        
        
        //oracle
        try 
        {
            DataflowContainer dataflowInfoR = elasticSearchClient.findById( DataflowContainer.class, dataflow.getMetadatas().get("esId"));
            assertNotNull(dataflowInfoR);
            assertEquals(DEFAULT_BODY_CONTENT, dataflowInfoR.getBody());
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
    }
    
    
    
    @Test
    void test_getDaoById_dataflowInfo_ok()
    {
        //preparation
        String correlationId = "123e4567-"+ java.util.Calendar.getInstance().getTimeInMillis();
        String archiveId = "ARCHID-"+ java.util.Calendar.getInstance().getTimeInMillis();
       
        DataflowInfo dataflowInfo = new DataflowInfo();
        dataflowInfo.setCorrelationId(correlationId);
        dataflowInfo.setContainerVersion("1.0");
        dataflowInfo.setType(DataFlowContainerType.TRANSPORT);
        dataflowInfo.setFrom("source");
        dataflowInfo.setTo("destination");
        dataflowInfo.setTimestamp(new Date());
        dataflowInfo.setArchiveId(correlationId+"_"+archiveId);
        dataflowInfo.setEntryPoint(new EntryPoint());
        Map<String, String> metadatas = new HashMap<>();
        metadatas.put("route", "maroute-"+java.util.Calendar.getInstance().getTimeInMillis());
        dataflowInfo.setMetadatas(metadatas);
        try 
        {
            dataflowInfo.getMetadatas().put("esId", elasticSearchClient.merge(dataflowInfo).getOid());
            assertTrue(StringUtil.isNotEmpty(dataflowInfo.getMetadatas().get("esId")), "L'enregistrement du DataflowInfoa échoué dans ES" );
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
        
        
        //oracle
        try 
        {
            DataflowInfo dataflowInfoR = elasticSearchClient.findById( DataflowInfo.class, dataflowInfo.getMetadatas().get("esId"));
            assertNotNull(dataflowInfoR);
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
    }
    
    
    
    @Test
    void test_getDaoByQuery_dataflowInfo_ok()
    {
        //preparation
        String correlationId = "123e4567-"+ java.util.Calendar.getInstance().getTimeInMillis();
        String archiveId = "ARCHID-"+ java.util.Calendar.getInstance().getTimeInMillis();
       
        DataflowInfo dataflowInfo = new DataflowInfo();
        dataflowInfo.setCorrelationId(correlationId);
        dataflowInfo.setContainerVersion("1.0");
        dataflowInfo.setType(DataFlowContainerType.TRANSPORT);
        dataflowInfo.setFrom("source");
        dataflowInfo.setTo("destination");
        dataflowInfo.setTimestamp(new Date());
        dataflowInfo.setArchiveId(correlationId+"_"+archiveId);
        dataflowInfo.setEntryPoint(new EntryPoint());
        Map<String, String> metadatas = new HashMap<>();
        metadatas.put("route", "getDaoByQuery_dataflowInfos-"+java.util.Calendar.getInstance().getTimeInMillis());
        dataflowInfo.setMetadatas(metadatas);
        try 
        {
            dataflowInfo.getMetadatas().put("esId", elasticSearchClient.merge(dataflowInfo).getOid());
            assertTrue(StringUtil.isNotEmpty(dataflowInfo.getMetadatas().get("esId")), "L'enregistrement du DataflowInfoa échoué dans ES" );
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
        
        
        //oracle
        try 
        {
            Query query = QueryBuilders.queryString(m -> m
                    .query("metadatas.route contains getDaoByQuery_dataflowInfos-17296105*")
            );
            PaginatedResult<DataflowInfo> dataflowInfos = elasticSearchClient.findByQuery( DataflowInfo.class, query, 0, 10);
            assertNotNull(dataflowInfos);
            assertTrue(dataflowInfos.getItems().size()>=1);
        } 
        catch (Exception e) 
        {
            fail("Une erreur est apparue.", e);
        }
    }
}
