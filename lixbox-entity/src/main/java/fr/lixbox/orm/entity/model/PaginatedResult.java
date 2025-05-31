/*******************************************************************************
 *    
 *                           FRAMEWORK Lixbox
 *                          ==================
 *      
 * This file is part of lixbox-orm.
 *
 *    lixbox-orm is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    lixbox-orm is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *    along with lixbox-orm.  If not, see <https://www.gnu.org/licenses/>
 *   
 *   @AUTHOR Lixbox-team
 *
 ******************************************************************************/
package fr.lixbox.orm.entity.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import fr.lixbox.common.util.CollectionUtil;

/**
 * Cette classe sert à paginer les résultats trop important.
 * 
 * @author ludovic.terral
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaginatedResult<T> implements Serializable
{
    // ----------- Attibuts -----------
    private static final long serialVersionUID = 202505192004L;
    @JsonProperty("items") private List<T> items;
    @JsonProperty("totalItems") private long totalItems = 0;
    @JsonProperty("offset") private long offset = 0;
    @JsonProperty("pageSize") private int pageSize = 100;
    
        
    
    //----------- Methodes -----------
    public PaginatedResult()
    {
    }



    public PaginatedResult(List<T> items, long totalItems, int offset, int pageSize)
    {
        this.items = items;
        this.totalItems = totalItems;
        this.offset = offset;
        this.pageSize = pageSize;
    }



    public List<T> getItems()
    {
        if (CollectionUtil.isEmpty(items)) 
        {
            items = new ArrayList<>();
        }
        return items;
    }
    public void setItems(List<T> items)
    {
        this.items = items;
    }

    
    
    


    public long getTotalItems()
    {
        return totalItems;
    }
    public void setTotalItems(long totalItems)
    {
        this.totalItems = totalItems;
    }



    public int getPageSize()
    {
        return pageSize;
    }
    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
    }

  

    public long getOffset()
    {
        return offset;
    }
    public void setOffset(long offset)
    {
        this.offset = offset;
    }



    public int getTotalPages()
    {
        return (int) Math.ceil((double) totalItems / pageSize);
    }



    public int getPageNumber()
    {

        return (int) (offset / pageSize) + 1;
    }



    public boolean hasMorePages()
    {
        return getPageNumber() < getTotalPages();
    }
}
