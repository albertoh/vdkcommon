package cz.incad.vdkcommon.solr;

import cz.incad.vdkcommon.SolrIndexerCommiter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocumentList;

/**
 *
 * @author alberto
 */
public class IndexerQuery {

    public static void query(String q) {

    }

    public static SolrDocumentList queryOneField(String q, String[] fields, String[] fq) throws SolrServerException, IOException {
        SolrServer server = SolrIndexerCommiter.getServer();
        SolrQuery query = new SolrQuery();
        query.setQuery(q);
        query.setFilterQueries(fq);
        query.setFields(fields);
        query.setRows(100);
        QueryResponse rsp = server.query(query);
        return rsp.getResults();
    }
    private String xml(String q) throws MalformedURLException, IOException {
        SolrQuery query = new SolrQuery(q);
// set indent == true, so that the xml output is formatted
        query.set("indent", true);

// use org.apache.solr.client.solrj.util.ClientUtils 
// to make a URL compatible query string of your SolrQuery
        String urlQueryString = ClientUtils.toQueryString(query, false);

        String solrURL = "http://localhost:8080/solr/shard-1/select";
        URL url = new URL(solrURL + urlQueryString);

// use org.apache.commons.io.IOUtils to do the http handling for you
        String xmlResponse = IOUtils.toString(url);

        return xmlResponse;
    }

}
