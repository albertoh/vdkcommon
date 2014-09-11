package cz.incad.vdkcommon.solr;

import cz.incad.vdkcommon.SolrIndexerCommiter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;

/**
 *
 * @author alberto
 */
public class IndexerQuery {

    public static void query(String q) {

    }

    public static String queryOneField(String q, String field) throws SolrServerException {
        SolrServer server = SolrIndexerCommiter.getServer();
        SolrQuery query = new SolrQuery();
        query.setQuery(q);
        QueryResponse rsp = server.query(query);
        SolrDocumentList docs = rsp.getResults();

        Iterator<SolrDocument> iter = docs.iterator();

        if (iter.hasNext()) {
            SolrDocument resultDoc = iter.next();

            return (String) resultDoc.getFirstValue(field);
        } else {
            return null;
        }
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
