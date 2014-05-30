package cz.incad.vdkcommon;

import cz.incad.utils.RomanNumber;
import cz.incad.vdkcommon.xml.XMLReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.validator.ISBNValidator;

/**
 *
 * @author alberto
 */
public class Slouceni {
    
    static final Logger logger = Logger.getLogger(Slouceni.class.getName());
    public static String generateMD5(String xml){
        try {
            XMLReader xmlReader = new XMLReader();
            xmlReader.loadXml(xml);
            //ISBN
            String pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='020']/marc:subfield[@code='a']/text()");
            pole = pole.toUpperCase();
            //logger.log(Level.INFO, "isbn: {0}", pole);
            ISBNValidator val =  new ISBNValidator();
            if(!"".equals(pole) && val.isValid(pole)){
                return MD5.generate(new String[]{pole});
            }
            
            //ISSN
            pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='022']/marc:subfield[@code='a']/text()");
            pole = pole.toUpperCase();
            //logger.log(Level.INFO, "issn: {0}", pole);
            if(!"".equals(pole) && val.isValid(pole)){
                return MD5.generate(new String[]{pole});
            }
            
            //ccnb
            pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='015']/marc:subfield[@code='a']/text()");
           //logger.log(Level.INFO, "ccnb: {0}", pole);
           if (!"".equals(pole)) {
                return MD5.generate(new String[]{pole});
            }
            
            //Check 245n číslo části 
            String f245nraw = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='n']/text()");
            //logger.log(Level.INFO, "245n číslo části: {0}", f245nraw);
            String f245n = "";
            RomanNumber rn = new RomanNumber(f245nraw);
            if(rn.isValid()){
                f245n = Integer.toString(rn.toInt());
            }
            
            //Pole 250 údaj o vydání (nechat pouze numerické znaky) (jen prvni cislice)
            String f250a = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='250']/marc:subfield[@code='a']/text()");
            //logger.log(Level.INFO, "f250a: {0}", f250a);
            f250a = onlyLeadNumbers(f250a);
            
            //Pole 100 autor – osobní jméno (ind1=1 →  prijmeni, jmeno; ind1=0 → jmeno, prijmeni.  
            //Obratit v pripade ind1=1, jinak nechat)
            String f100a = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='100']/marc:subfield[@code='a']/text()");
            String ind1 = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='100']/@ind1");
            if("1".equals(ind1) && !"".equals(f100a)){
                String[] split = f100a.split(",", 2);
                if(split.length == 2){
                    f100a = split[1] + split[0];
                }
            }
            
            //vyber poli
            String uniqueCode = MD5.generate(new String[]{
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='a']/text()"),
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='b']/text()"),
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='c']/text()"),
                    f245n,
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='p']/text()"),
                    f250a,
                    f100a,
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='110']/marc:subfield[@code='a']/text()"),
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='111']/marc:subfield[@code='a']/text()"),
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='260']/marc:subfield[@code='a']/text()"),
                    xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='260']/marc:subfield[@code='b']/text()"),
                    onlyLeadNumbers(xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='260']/marc:subfield[@code='c']/text()"))
                });

            return uniqueCode;
        } catch (Exception ex) {
           logger.log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private static String onlyLeadNumbers(String s) {
        String retVal = "";
        int n = 0;
        while(n < s.length() && Character.isDigit(s.charAt(n)) ){
            retVal += s.charAt(n);
            n++;
        }
        return retVal;
    }
    
}
