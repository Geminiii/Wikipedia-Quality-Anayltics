/**
 * Page in the file
 */
public class Page {
    /**
     * Article's  id
     */
    private int id;
    /**
     * Namespace of this article
     */
    private int ns;
    /**
     * Article's title
     */
    private String title;

    /**
     * The redirect article of this article
     */
    private String redirect;

    Page(){
        id = 0;
        ns = 0;
        setRedirect(null);
        setTitle(null);
    }

    void setId(String id){
        this.id = Integer.parseInt(id);
    }

    int getId(){
        return this.id;
    }

    void setNs(String ns){
        this.ns = Integer.parseInt(ns);
    }

    int getNs(){
        return this.ns;
    }

    void setTitle(String title){
        this.title = title;
    }

    void setRedirect(String redirect){
        this.redirect = redirect;
    }

    @Override
    public String toString(){
        if(this.ns != 0) return "";
        StringBuilder sb = new StringBuilder();
        sb.append(this.id).append('\t').append(this.title);
        if(this.redirect != null ) sb.append(',').append(this.redirect);
        return sb.append('\n').toString();
    }

}
