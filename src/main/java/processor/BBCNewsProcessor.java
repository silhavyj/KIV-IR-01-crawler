package processor;

import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;

import java.util.List;
import java.util.Optional;

/***
 * @author silhavyj
 * This class respresents a parser that parses an article from
 * www.bbc.com/news. The final format of the article is JSON.
 *
 * {
 *   "title",    - obligatory
 *   "author",   - optional
 *   "datetime", - obligatory
 *   "article",  - obligatory
 *   "subject"   - optional
 * }
 */
public class BBCNewsProcessor implements ISiteProcessor {

    /*** XPATH of the title of an article. */
    private static final String TITLE_XPATH = "//*[@id=\"main-heading\"]";

    /*** XPATH of the author of an article. */
    private static final String AUTHOR_XPATH = "//*[@id=\"main-content\"]/div[5]/div/div[1]/article/header/p/span/strong";

    /*** XPATH of the datetime (time of creation) of an article. */
    private static final String DATETIME_XPATH = "//*[@id=\"main-content\"]/div[5]/div/div[1]/article/header/div[1]/dl/div[1]/dd/span/time";

    /*** XPATH of the subject of an article. */
    private static final String SUBJECT_XPATH = "//*[@id=\"main-content\"]/div[5]/div/div[1]/article/header/div[2]/div[2]/div/ul/li/a";

    /*** XPATH of all paragraphs that make up an article. */
    private static final String ARTICLE_PARAGRAPHS_XPATH = "//*[@id=\"main-content\"]/div[5]/div/div[1]/article/div[*]/div/p";

    /*** Attribute holding the actual datetime. */
    private static final String DATETIME_ATTRIBUTE = "datetime";

    /*** Prefix that some authors' names start with (e.g. By John Smith). */
    private static final String AUTHOR_PREFIX = "By ";

    /*** Title key in the JSON object. */
    private static final String JSON_TITLE_KEY = "title";

    /*** Author key in the JSON object. */
    private static final String JSON_AUTHOR_KEY = "author";

    /*** Datetime key in the JSON object. */
    private static final String JSON_DATETIME_KEY = "datetime";

    /*** Article key in the JSON object. */
    private static final String JSON_ARTICLE_KEY = "article";

    /*** Subject key in the JSON object. */
    private static final String JSON_SUBJECT_KEY = "subject";

    /*** Document to be analyzed. */
    private Document document;

    /***
     * Finds the title of an article. It uses an XPATH to find it.
     * @return If the title is found, it will be returned. Otherwise, null will be returned.
     */
    private String processTitle() {
        var title = document.selectXpath(TITLE_XPATH);
        if (title.size() == 1) {
            return title.get(0).childNode(0).toString().strip();
        }
        return null;
    }

    /***
     * Finds the datetime of an article. It uses an XPATH to find it.
     * @return If the datetime is found, it will be returned. Otherwise, null will be returned.
     */
    private String processDatetime() {
        var datetime = document.selectXpath(DATETIME_XPATH);
        if (datetime.size() == 1) {
            return datetime.get(0).attr(DATETIME_ATTRIBUTE);
        }
        return null;
    }

    /***
     * Finds the author of an article. It uses an XPATH to find it.
     * @return If the author is found, it will be returned. Otherwise, null will be returned.
     */
    private String processAuthor() {
        var author = document.selectXpath(AUTHOR_XPATH);
        if (author.size() == 1) {
            var name = author.get(0).childNode(0).toString().strip();

            // If the name starts with 'By ', remove it.
            if (name.startsWith(AUTHOR_PREFIX)) {
                name = name.substring(AUTHOR_PREFIX.length());
            }

            // The name may also be bold or highlighted, so
            // retrieve the plain text only.
            name = Jsoup.parse(name).text();
            return name;
        }
        return null;
    }

    /***
     * Finds an article in the document. It uses an XPATH to find it.
     * @return If an article is found, it will be returned. Otherwise, null will be returned.
     */
    private String processArticle() {
        // Find all paragraphs that make up the article.
        var article = document.selectXpath(ARTICLE_PARAGRAPHS_XPATH);

        if (article.size() > 0) {
            var sb = new StringBuilder();
            List<Node> childNodes;

            for (var paragraph : article) {
                childNodes = paragraph.childNodes();
                for (var pieceOfText : childNodes) {
                    // Get rid of any additional HTML that may still be in the text.
                    sb.append(Jsoup.parse(pieceOfText.toString()).text());

                    // Make sure that there is a space at the end of each article.
                    if (!pieceOfText.toString().endsWith(" "))
                        sb.append(" ");
                }
            }
            return sb.toString();
        }
        return null;
    }

    /***
     * Finds the topic of an article. It uses an XPATH to find it.
     * @return If the topic is found, it will be returned. Otherwise, null will be returned.
     */
    private String processTopic() {
        var subject = document.selectXpath(SUBJECT_XPATH);
        if (subject.size() == 1) {
            return subject.get(0).childNode(0).toString().strip();
        }
        return null;
    }

    /***
     * Parses a document. It tries to find out whether there is an article
     * in the document or not. If there is an article, it will fetch it and
     * convert it into a JSON format.
     * @param document Document to be parsed
     * @return Optional of a JSONObject - it could be empty in case that no article has been found.
     */
    @Override
    public Optional<JSONObject> processSite(Document document) {
        this.document = document;
        try {
            // Find all metadata along with the article itself.
            var title = processTitle();
            var datetime = processDatetime();
            var author = processAuthor();
            var article = processArticle();
            var subject = processTopic();

            // Make sure that the title, datetime, and article are present.
            // Otherwise, do not consider it as an article.
            if (title != null && datetime != null && article != null) {
                JSONObject jsonObject = new JSONObject();

                jsonObject.put(JSON_TITLE_KEY, title);
                jsonObject.put(JSON_DATETIME_KEY, datetime);
                jsonObject.put(JSON_ARTICLE_KEY, article);

                // If there's also an author, add it to the object as well.
                if (author != null) {
                    jsonObject.put(JSON_AUTHOR_KEY, author);
                }

                // If there's also a subject, add it to the object as well.
                if (subject != null) {
                    jsonObject.put(JSON_SUBJECT_KEY, subject);
                }

                // Return the object.
                return Optional.of(jsonObject);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return Optional.empty();
    }
}
