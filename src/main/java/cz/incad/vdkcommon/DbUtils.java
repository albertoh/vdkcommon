

package cz.incad.vdkcommon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 *
 * @author alberto
 */
public class DbUtils {
    public enum Roles{
        ADMIN ("admin"),
        SOURCELIB ("sourcelib"),
        USER ("user"),
        LIB ("lib");
        
        String value;
        Roles(String value){
            this.value = value;
        }
        
        @Override
        public String toString(){
            return this.value;
        }
    };
    
    public static Connection getConnection() throws NamingException, SQLException {
        Context initContext = new InitialContext();
        Context envContext = (Context) initContext.lookup("java:/comp/env");
        DataSource ds = (DataSource) envContext.lookup("jdbc/vdk");
        return ds.getConnection();
    }
    public static Connection getConnection(String className,
        String url, String username, String password) throws NamingException, SQLException, ClassNotFoundException {
        Class.forName(className);
        return DriverManager.getConnection(url, username, password);
    }
    public static String getXml(String id) throws SQLException{
        Connection conn = null;
        try {
            
            conn = DbUtils.getConnection();

            String sql = "select sourceXML from ZAZNAM where identifikator=?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }else{
                return "<xml/>";
            }

        } catch (Exception ex) {
            return ex.toString();
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}
