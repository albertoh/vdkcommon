package cz.incad.vdkcommon.db;

import cz.incad.vdkcommon.DbUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author alberto
 */
public class Zaznam {

    private static final Logger logger = Logger.getLogger(Zaznam.class.getName());

    public String typDokumentu;
    public String hlavniNazev;
    public String ccnb;
    public String identifikator;
    public String urlZdroje;
    public String sourceXML;
    public String uzivatel;
    public String knihovna;
    public String uniqueCode;
    public String codeType;
    public boolean bohemika;
    
    PreparedStatement psInsert;
    PreparedStatement psUpdate;
    
    public Zaznam(Connection conn) throws SQLException{
        StringBuilder sql = new StringBuilder("insert into ZAZNAM ");
        boolean isOracle = DbUtils.isOracle(conn);
        
        sql.append(" (knihovna, identifikator, uniqueCode, codeType, url, hlavniNazev, typDokumentu, bohemika, sourceXML, update_timestamp");
        if(isOracle){
            sql.append(",zaznam_id");
        }
        sql.append(")");
        sql.append(" values (?,?,?,?,?,?,?,?,?");
        if(isOracle){
            sql.append(", sysdate, Zaznam_ID_SQ.nextval");
        }else{
            sql.append(",NOW()");
            
        }
        sql.append(")");
        psInsert = conn.prepareStatement(sql.toString());
        
        StringBuilder sqlUpdate = new StringBuilder("update ZAZNAM ");
        sqlUpdate.append("set knihovna=?, identifikator=?, ")
                .append("uniqueCode=?, codeType=?, url=?, hlavniNazev=?, typDokumentu=?, bohemika=?, ")
                .append("sourceXML=?, update_timestamp=");
        
        if(isOracle){
            sqlUpdate.append("sysdate");
        }else{
            sqlUpdate.append("NOW()");
        }
        sqlUpdate.append(" where zaznam_id=?");
        psUpdate = conn.prepareStatement(sqlUpdate.toString());
    }
    
    public void clearParams() throws SQLException{
        psUpdate.clearParameters();
        psInsert.clearParameters();
    }
    
    public void update(int zaznamId) throws SQLException {
        psUpdate.setInt(psUpdate.getParameterMetaData().getParameterCount(), zaznamId);
        process(psUpdate);
    }

    public void insert() throws SQLException {
            process(psInsert);
    }

    public void process(PreparedStatement ps) throws SQLException {
        int i = 1;
        ps.setString(i++, knihovna);
        ps.setString(i++, identifikator);
        ps.setString(i++, uniqueCode);
        ps.setString(i++, codeType);
        ps.setString(i++, urlZdroje);
        ps.setString(i++, hlavniNazev);
        ps.setString(i++, typDokumentu);
        ps.setBoolean(i++, bohemika);
        ps.setString(i++, sourceXML);
        logger.log(Level.FINE, "executing sql {0}...", ps.toString());
        ps.executeUpdate();
    }
}
