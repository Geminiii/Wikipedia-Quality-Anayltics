import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * Wikipedia XML files parser
 */
public class Parser {
    public static void main(String[] args){
        String PATH = "/Users/Li/Downloads/raw/";
        for(int i = 21; i<28; i++){
            File f = new File(PATH + "enwiki-20161101-stub-meta-history"+ i + ".xml");
            SAXParserFactory spf = SAXParserFactory.newInstance();

            try(InputStream is = new FileInputStream(f)
            ){
                SAXParser parser = spf.newSAXParser();
                DefaultHandler handler = new UserHandler();
                long millis = System.currentTimeMillis();
                parser.parse(is, handler);
                System.out.print("Finished in seconds: " + (System.currentTimeMillis() - millis) % 1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}

/**
 * The UserHandler class that used by the Parser class
 */
class UserHandler extends DefaultHandler{
    private boolean bPage = false;
    private boolean bTitle = false;
    private boolean bNs = false;
    private boolean bId = false;
    private boolean bRedirect = false;
    private boolean bRevision = false;
    private boolean bTimeStamp = false;
    private boolean bContributor = false;
    private boolean bUsername = false;
    private boolean bIp = false;
    private boolean bMinor = false;

    private Page page = null;
    private Revision revision = null;
    private Contributor contributor = null;
    private RevisionList revisionlist = null;

    private BufferedWriter pageWriter = getBW("page");
    private BufferedWriter contributorWriter = getBW("contributor");
    private BufferedWriter revisionListWriter = getBW("revisionList");

    /**
     * Returns BufferedWriter for the parsing output
     * @param filename The file name the writer writing to
     * @return BufferedWriter for the parsing output
     */
    private BufferedWriter getBW(String filename) {
        String path = "/Users/Li/Downloads/parsed/";
        File f = new File(path + filename + System.currentTimeMillis());
        BufferedWriter bw =null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bw;
    }

    /**
     * Write the Contributors list of this file into disk.
     */
    private void dumpContributors(){
        TreeMap<String, List<String>> allContributors = Contributor.allContributors;
        List<String> l;
        for(Map.Entry<String,List<String>> entry: allContributors.entrySet()){
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey().trim()).append("\t");
            String prefix = "";
            l = entry.getValue();
            for(String s : l){
                if(s != null && !s.trim().equals("")){
                    sb.append(prefix).append(s);
                    prefix = "|";
                }
            }
            try{
                contributorWriter.write(sb.append('\n').toString());
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        Contributor.allContributors = new TreeMap<>();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts)
            throws SAXException{
        if(qName.equalsIgnoreCase("page")){
            page = new Page();
            revisionlist = new RevisionList();
            bPage = true;
        }else if(qName.equalsIgnoreCase("title")){
            bTitle = true;
        }else if(qName.equalsIgnoreCase("ns")){
            bNs= true;
        }else if(qName.equalsIgnoreCase("id")){
            bId = true;
        }else if(qName.equalsIgnoreCase("redirect")){
            String redirect = atts.getValue("title");
            page.setRedirect(redirect);
        }else if(qName.equalsIgnoreCase("revision")){
            revision = new Revision();
            bRevision = true;
        }else if(qName.equalsIgnoreCase("timestamp")){
            bTimeStamp = true;
        }else if(qName.equalsIgnoreCase("contributor")){
            contributor = new Contributor();
            bContributor = true;
        }else if(qName.equalsIgnoreCase("username")){
            bUsername = true;
        }else if(qName.equalsIgnoreCase("ip")){
            bIp = true;
        }else if(qName.equalsIgnoreCase("minor")){
            revision.setMinor();
        }else if(qName.equalsIgnoreCase("text")){
            String length = atts.getValue("bytes");
            revision.setLength(length);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        if(qName.equalsIgnoreCase("contributor")){

            bContributor = false;
        }else if(qName.equalsIgnoreCase("revision")){
            bRevision = false;
            try{
                revisionlist.addRevision(revision);
            }catch (Exception e){
                e.printStackTrace();
            }
        }else if(qName.equalsIgnoreCase("page")){
            bPage = false;
            try{
                String pageStr = page.toString();
                pageWriter.write(pageStr);
                //System.out.print(pageStr);
                revisionListWriter.write(revisionlist.toString());
            }catch (Exception e){
                e.printStackTrace();
            }
        }else if(qName.equalsIgnoreCase("mediawiki")){
            try{
                dumpContributors();
                contributorWriter.close();
                pageWriter.close();
                //revisionWriter.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
        String content = new String(ch, start, length);
        if(bTitle){
            page.setTitle(content);
            bTitle = false;
        }else if(bNs){
            page.setNs(content);
            bNs = false;
        }else if(bId){
            if(bContributor){
                contributor.setId(content);
                contributor.record();
                revision.setContributor(contributor);
            }else if(bRevision){
                revision.setId(content);
                revision.setPageID(page.getId());
                revision.setNs(page.getNs());
            }else{
                page.setId(content);
                revisionlist.setId(Integer.parseInt(content));
            }
            bId = false;
        }else if(bTimeStamp){
            revision.setTimestamp(content);
            bTimeStamp = false;
        }else if(bUsername){
            contributor.setUsername(content);
            bUsername = false;
        }else if(bIp){
            contributor.setId(content);
            //contributor.setUsername(content);
            contributor.record();
            revision.setContributor(contributor);
            bIp = false;
        }
    }

}
