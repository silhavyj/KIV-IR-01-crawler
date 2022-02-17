package crawler;

import org.jsoup.nodes.Document;
import processor.ISiteProcessor;

import java.util.Optional;

/***
 * @author silhavyj
 *
 * This class represents a worker that tries to parse a document fetched down
 * by the crawler. Parsing a document means tring to determine whether there is an
 * article in the document or not. If an article is present, it will add it to the
 * list of final results held in the crawler.
 */
public class WebCrawlerWorker extends Thread {

    /*** Key of the URL added to the JSON object that represents an article. */
    public static final String URL_JSON_KEY = "url";

    /***
     * Amount of millisecond for which the worker will sleep if the
     * crawler has returned an empty document to parse (there's a delay
     * before asking again).
     * */
    private static final int SLEEP_DELAY_MS = 10;

    /*** ID of the worker. */
    private final int workerId;

    /*** Reference to the webcrawler. */
    private final IWebCrawler webCrawler;

    /*** Reference to a site processor used to look for X-PATHs. */
    private final ISiteProcessor siteProcessor;

    /***
     * Creates an instance of the class.
     * @param workerId ID of the worker.
     * @param webCrawler Reference to the webcrawler.
     * @param siteProcessor Reference to a site processor used to look for X-PATHs.
     */
    public WebCrawlerWorker(final int workerId, final IWebCrawler webCrawler, final ISiteProcessor siteProcessor) {
        this.workerId = workerId;
        this.webCrawler = webCrawler;
        this.siteProcessor = siteProcessor;
    }

    /***
     * If the crawler returns an empty document to parse, the worker
     * waits for a certain amount of time before asking it again.
     * */
    private void takeBreak() {
        try {
            Thread.sleep(SLEEP_DELAY_MS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /***
     * Processes a document. It uses the site processor to parse the website,
     * if the parsing is successful, the article is added to the final list of
     * all articles held in the crawler.
     * @param document document to be parsed
     */
    private void processDocument(final Document document) {
        // Ask the site processor to parse the document.
        var data = siteProcessor.processSite(document);

        // If an article has been found, add the URL to the JSON object,
        // and add it to the final list of all articles.
        if (data.isPresent()) {
            data.get().put(URL_JSON_KEY, document.location());
            webCrawler.appendFetchedData(workerId, data.get());
        }
    }

    /***
     * Starts asking the crawling for documents to
     * parse until the crawl has finished.
     */
    @Override
    public void run() {
        Optional<Document> document;

        // Keep on asking for a document until the crawl is done.
        while (!webCrawler.isFinished()) {
            document = webCrawler.getDocumentToProcess();

            // If the document is empty, sleep for a while before
            // asking again. Otherwise, try to parse the document.
            if (document.isPresent()) {
                processDocument(document.get());
            } else {
                takeBreak();
            }
        }
    }
}
