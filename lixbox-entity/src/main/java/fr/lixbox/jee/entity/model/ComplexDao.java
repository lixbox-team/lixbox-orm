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
 ******************************************************************************/
package fr.lixbox.jee.entity.model;

import java.io.Serializable;


/**
 * Cette interface represente les attributs necessaire a une entite pour
 * pouvoir etre utilise par le manager de DAO.
 * 
 * @author ludovic.terral
 */
public interface ComplexDao extends Serializable, ExtendDao
{    
    // ----------- Methode -----------
    void setHasInit(boolean hasInit);
}
