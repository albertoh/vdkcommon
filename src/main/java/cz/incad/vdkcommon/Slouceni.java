package cz.incad.vdkcommon;

import cz.incad.utils.RomanNumber;
import cz.incad.vdkcommon.xml.XMLReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.validator.routines.ISBNValidator;
import org.json.JSONObject;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;

/**
 *
 * @author alberto
 */
public class Slouceni {

    static final Logger logger = Logger.getLogger(Slouceni.class.getName());

    public static String export(String xml) {

        try {
            String retval = "";
            XMLReader xmlReader = new XMLReader();
            xmlReader.loadXml(xml);

            //ISBN
            String pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='020']/marc:subfield[@code='a']/text()");
            retval += pole + "\t";

            //ISSN
            pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='022']/marc:subfield[@code='a']/text()");
            retval += pole + "\t";

            //ccnb
            pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='015']/marc:subfield[@code='a']/text()");
            retval += pole + "\t";

            //Check 245n číslo části 
            String f245nraw = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='n']/text()");

            //Pole 250 údaj o vydání (nechat pouze numerické znaky) (jen prvni cislice)
            String f250a = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='250']/marc:subfield[@code='a']/text()");

            //Pole 100 autor – osobní jméno (ind1=1 →  prijmeni, jmeno; ind1=0 → jmeno, prijmeni.  
            //Obratit v pripade ind1=1, jinak nechat)
            String f100a = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='100']/marc:subfield[@code='a']/text()");
            String ind1 = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='100']/@ind1");
            if ("1".equals(ind1) && !"".equals(f100a)) {
                String[] split = f100a.split(",", 2);
                if (split.length == 2) {
                    f100a = split[1] + split[0];
                }
            }

            //vyber poli
            retval += xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='a']/text()")
                    + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='b']/text()") + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='c']/text()") + "\t"
                    + f245nraw + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='p']/text()") + "\t"
                    + f250a + "\t"
                    + f100a + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='110']/marc:subfield[@code='a']/text()") + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='111']/marc:subfield[@code='a']/text()") + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='260']/marc:subfield[@code='a']/text()") + "\t"
                    + xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='260']/marc:subfield[@code='b']/text()") + "\t"
                    + onlyLeadNumbers(xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='260']/marc:subfield[@code='c']/text()")) + "\t";

            return retval;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static Map csvToMap(String[] parts) {
        Map<String, String> map = new HashMap<String, String>();

        map.put("isbn", parts[0]);
        map.put("issn", parts[1]);
        map.put("ccnb", parts[2]);
        map.put("245a", parts[3]);
        map.put("245n", parts[4]);
        map.put("245p", parts[5]);
        map.put("250a", parts[6]);
        map.put("100a", parts[7]);
        map.put("110a", parts[8]);
        map.put("111a", parts[9]);
        map.put("260a", parts[10]);
        return map;
    }

    public static String csvToJSONString(String csv) {
        try {
            CSVStrategy strategy = new CSVStrategy('\t', '\"', '#');
            CSVParser parser = new CSVParser(new StringReader(csv), strategy);
            String[] parts = parser.getLine();
            if (parts != null) {
                return toJSON(csvToMap(parts)).toString();
            }
        } catch (IOException ex) {
            Logger.getLogger(Slouceni.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }

    public static String generateMD5(String[] parts) {

        try {

            //ISBN
            String pole = parts[0];
            //logger.log(Level.INFO, "isbn: {0}", pole);
            ISBNValidator val = new ISBNValidator();
            if (!"".equals(pole) && val.isValid(pole)) {
                pole = pole.toUpperCase();
                return MD5.generate(new String[]{pole});
            }

            //ISSN
            pole = parts[1];
            //logger.log(Level.INFO, "issn: {0}", pole);
            if (!"".equals(pole) && val.isValid(pole)) {
                pole = pole.toUpperCase();
                return MD5.generate(new String[]{pole});
            }

            //ccnb
            pole = parts[3];
            //logger.log(Level.INFO, "ccnb: {0}", pole);
            if (!"".equals(pole)) {
                return MD5.generate(new String[]{pole});
            }

            //vyber poli
            return MD5.generate(Arrays.copyOfRange(parts, 4, 10));
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static JSONObject toJSON(Map<String, String> map) {
        try {
            JSONObject j = new JSONObject(map);
            j.put("docCode", generateMD5(map));
            return j;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static String generateMD5(Map<String, String> map) {
        try {

            //ISBN
            String pole = map.get("isbn");
            pole = pole.toUpperCase().substring(0, Math.min(13, pole.length()));
            //logger.log(Level.INFO, "isbn: {0}", pole);
            ISBNValidator val = new ISBNValidator();
            if (!"".equals(pole) && val.isValid(pole)) {
                return MD5.generate(new String[]{pole});
            }

            //ISSN
            pole = map.get("issn");
            pole = pole.toUpperCase().substring(0, Math.min(13, pole.length()));
            //logger.log(Level.INFO, "issn: {0}", pole);
            if (!"".equals(pole) && val.isValid(pole)) {
                return MD5.generate(new String[]{pole});
            }

            //ccnb
            pole = map.get("ccnb");
            //logger.log(Level.INFO, "ccnb: {0}", pole);
            if (!"".equals(pole)) {
                return MD5.generate(new String[]{pole});
            }

            //Check 245n číslo části 
            String f245nraw = map.get("245n");
            //logger.log(Level.INFO, "245n číslo části: {0}", f245nraw);
            String f245n = "";
            RomanNumber rn = new RomanNumber(f245nraw);
            if (rn.isValid()) {
                f245n = Integer.toString(rn.toInt());
            }

            //Pole 250 údaj o vydání (nechat pouze numerické znaky) (jen prvni cislice)
            String f250a = map.get("250a");
            //logger.log(Level.INFO, "f250a: {0}", f250a);
            f250a = onlyLeadNumbers(f250a);

            //Pole 100 autor – osobní jméno (ind1=1 →  prijmeni, jmeno; ind1=0 → jmeno, prijmeni.  
            //Obratit v pripade ind1=1, jinak nechat)
            String f100a = map.get("100a");
            String ind1 = map.get("100aind1");
            if ("1".equals(ind1) && !"".equals(f100a)) {
                String[] split = f100a.split(",", 2);
                if (split.length == 2) {
                    f100a = split[1] + split[0];
                }
            }

            if ("".equals(f100a)) {
                f100a = map.get("245c");
            }

            //vyber poli
            String uniqueCode = MD5.generate(new String[]{
                map.get("245a"),
                map.get("245b"),
                //map.get("245c"),
                f245n,
                map.get("245p"),
                f250a,
                f100a,
                map.get("110a"),
                map.get("111a"),
                map.get("260a"),
                map.get("260b"),
                onlyLeadNumbers(map.get("260c"))
            });

            return uniqueCode;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static String generateMD5(String xml) {
        try {
            XMLReader xmlReader = new XMLReader();
            xmlReader.loadXml(xml);

            //ISBN
            String pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='020']/marc:subfield[@code='a']/text()");
            pole = pole.toUpperCase();
            //logger.log(Level.INFO, "isbn: {0}", pole);
            ISBNValidator val = new ISBNValidator();
            if (!"".equals(pole) && val.isValid(pole)) {
                return MD5.generate(new String[]{pole});
            }

            //ISSN
            pole = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='022']/marc:subfield[@code='a']/text()");
            pole = pole.toUpperCase();
            //logger.log(Level.INFO, "issn: {0}", pole);
            if (!"".equals(pole) && val.isValid(pole)) {
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
            if (rn.isValid()) {
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
            if ("1".equals(ind1) && !"".equals(f100a)) {
                String[] split = f100a.split(",", 2);
                if (split.length == 2) {
                    f100a = split[1] + split[0];
                }
            }
            if ("".equals(f100a)) {
                f100a = xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='c']/text()");
            }

            //vyber poli
            String uniqueCode = MD5.generate(new String[]{
                xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='a']/text()"),
                xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='b']/text()"),
                //xmlReader.getNodeValue("/oai:record/oai:metadata/marc:record/marc:datafield[@tag='245']/marc:subfield[@code='c']/text()"),
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
        if (s == null || "".equals(s)) {
            return s;
        }
        String retVal = "";
        int n = 0;
        while (n < s.length() && Character.isDigit(s.charAt(n))) {
            retVal += s.charAt(n);
            n++;
        }
        return retVal;
    }

}
