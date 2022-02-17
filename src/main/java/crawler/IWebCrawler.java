package crawler;

import org.json.JSONObject;
import org.jsoup.nodes.Document;

import java.time.LocalDateTime;
import java.util.Optional;

/***
 * @author silhavyj
 *
 * This interface defines the functionality of a general crawler.
 */
public interface IWebCrawler {

    /***
     * Starts the crawl.
     */
    void crawl();

    /***
     * Returns boolean value indicating whether the crawl has already finished or not.
     * @return true, if there are no more websites to crawl as well as no more documents
     *         waiting to be processed by one of the workers. False, otherwise.
     */
    boolean isFinished();

    /***
     * Returns the timestamp when the crawl started.
     * @return the start time of the crawl
     */
    LocalDateTime getStartedCrawlingTime();

    /***
     * Returns the timestamp when the crawl finished.
     * @return the finish time of the crawl
     */
    LocalDateTime getFinishedCrawlingTime();

    /***
     * Appends parsed data into the final list of all articles. This method is called
     * by the workers when they successfully manage to parse a website. They will
     * return a JSON object representing an article.
     * @param workerId id of the worker who has successfully parsed a document
     * @param data JSON object representing an article
     */
    void appendFetchedData(final int workerId, final JSONObject data);

    /***
     * Returns a document to process. This method is called by the workers
     * when they want to parse (analyze) a document (website).
     * @return Optional of a document. If there isn't any document to be parsed, the
     *         optional is empty.
     */
    Optional<Document> getDocumentToProcess();
}
