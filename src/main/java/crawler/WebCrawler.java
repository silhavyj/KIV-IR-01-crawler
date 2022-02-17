package crawler;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import processor.ISiteProcessor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.*;

import static crawler.WebCrawlerWorker.URL_JSON_KEY;

/***
 * @author silhavyj
 *
 * This class represents an implementation of a web crawler as defined in IWebCrawler.
 */
public class WebCrawler implements IWebCrawler {

    /*** Instance of a logger (used to print out information as the crawler crawls the web). */
    static Logger logger = Logger.getLogger(WebCrawler.class);

    /*** Name (format) of a dump file. */
    private static final String DUMP_FILENAME = "crawl-dump-%d-%s.json";

    /*** Name of the final file containing the results of the crawl. */
    private static final String FINAL_RESULTS_FILENAME = "crawl-results.json";

    /***
     * Number of milliseconds for which the crawler will sleep after
     * fetching down a website. Its main purpose is to prevent 'bombarding'
     * the website with requests.
     * */
    private static final int POLITENESS_PERIOD_MS = 10;

    /***
     * Hash mark symbol. It's removed from every URL as it only refers to
     * different paragraphs within the same site.
     * */
    private static final char HASH_MARK_SYMBOL = '#';

    /*** CSS query for finding all 'ahrefs' (links) on a website. */
    private static final String HREF_CSS_QUERY = "a[href]";

    /*** Attribute of an ahref element holding the link's URL. */
    private static final String HREF_ATTRIBUTE_KEY = "href";

    /*** The base URL from which the crawl starts crawling. */
    private final String rootUrl;

    /*** Maximum depth at which the crawler goes. */
    private final int maxDepth;

    /*** Dump period. For example, a dump file will be created after every 100 articles. */
    private final int dumpLimit;

    /*** Collection of all workers. */
    private List<WebCrawlerWorker> workers;

    /*** Set of all visited URLs. */
    private Set<String> visitedUrls;

    /*** Collection of documents that have been fetched down but are yet to be processed by a worker. */
    private Set<Document> documentsToProcess;

    /*** JSON array of all successfully parsed articles. */
    private JSONArray fetchedData;

    /*** Start time of the crawl. */
    private LocalDateTime startedCrawling;

    /*** Finish time of the crawl. */
    private LocalDateTime finishedCrawling;

    /*** Flag if the crawl has finished. */
    private boolean crawlFinished;

    /***
     * Creates an instance of the class.
     * @param rootUrl The base URL from which the crawl starts crawling.
     * @param maxDepth Maximum depth at which the crawler goes.
     * @param dumpLimit Dump period.
     * @param siteProcessors Collection of site processors that will be passed into the workers upon their initialization.
     */
    public WebCrawler(String rootUrl, int maxDepth, int dumpLimit, List<ISiteProcessor> siteProcessors) {
        this.rootUrl = rootUrl;
        this.maxDepth = maxDepth;
        this.dumpLimit = dumpLimit;

        initWorkers(siteProcessors);
    }

    /***
     * Creates and initializes all workers. Each worker will be
     * assigned a site processor, the implementation (logic) of
     * parsing a website.
     * @param siteProcessors List of site processors.
     */
    private void initWorkers(final List<ISiteProcessor> siteProcessors) {
        int workerId = 1; // the first worker has id 1
        workers = new LinkedList<>();
        for (final var siteProcessor : siteProcessors) {
            if (siteProcessor != null) {
                workers.add(new WebCrawlerWorker(workerId, this, siteProcessor));
                workerId++;
            }
        }
    }

    /***
     * Starts off all workers to do their job.
     */
    private void startWorkers() {
        for (var worker : workers) {
            worker.start();
        }
    }

    /***
     * Downloads the content of a website identified by a URL.
     * @param url URL of the website that will be downloaded.
     * @return if it fails to download the content of the website, an empty optional
     *         will be returned. Otherwise, a document (content of the site) will be returned.
     */
    private Optional<Document> fetchSiteContent(final String url) {
        try {
            var connection = Jsoup.connect(url);
            var document = connection.get();
            return Optional.of(document);
        } catch (Exception e) {
            logger.warn(String.format("Could not fetch the content of %s", url));
        }
        return Optional.empty();
    }

    /***
     * Waits for all workers to finish.
     */
    private void waitForWorkers() {
        for (var worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /***
     * Initializes all collections and data structures used during the crawl.
     */
    private void initCrawl() {
        visitedUrls = new HashSet<>();
        documentsToProcess = new HashSet<>();
        fetchedData = new JSONArray();
        crawlFinished = false;
        visitedUrls.add(rootUrl);
        startedCrawling = LocalDateTime.now();
    }

    /***
     * Terminates the crawl. It stores how much time the
     * whole process took and sets up the flag, so the workers
     * can be terminated as well.
     * */
    private void terminateCrawl() {
        crawlFinished = true;
        finishedCrawling = LocalDateTime.now();
    }

    /***
     * Waits for a certain amount of milliseconds before
     * fetching down another site.
     * */
    private void applyPoliteness() {
        try {
            Thread.sleep(POLITENESS_PERIOD_MS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /***
     * Processes a site. It downloads the content of the site identified
     * by its URL, marks it so the workers can go ahead and parse it,
     * finds all links the site contains, and recursively does the same thing
     * with all the links it has found.
     * @param url
     * @param currentDepth
     */
    private void processSite(final String url, int currentDepth) {
        // Wait for a certain amount of milliseconds.
        applyPoliteness();

        // Download the content of the website.
        final var document = fetchSiteContent(url);

        // Check if the website has been downloaded successfully.
        if (document.isPresent()) {
            // Add the document to the collection of documents to be processed by workers.
            synchronized (documentsToProcess) {
                documentsToProcess.add(document.get());
            }

            logger.trace(String.format("[depth=%d] crawled %s", currentDepth, document.get().location()));

            // If the maximum depth hasn't been reached yet, find all links
            // on the website and recursively call itself.
            if (currentDepth < maxDepth) {
                visitNewSites(document.get(), currentDepth);
            }
        }
    }

    /***
     * Removes hash marks from a URL. A hash mark is just
     * a reference to a particular paragraph within the same site.
     * @param url URL from which all hash marks will be removed
     * @return URL without any hash marks
     */
    private String removeHashMarksFromURL(final String url) {
        int pos = url.indexOf(HASH_MARK_SYMBOL);
        if (pos != -1) {
            return url.substring(0, pos);
        }
        return url;
    }

    /***
     * Crawls new sites using links found in the current document (web page)
     * @param document current document
     * @param currentDepth current depth (number of links from the root website)
     */
    private void visitNewSites(final Document document, int currentDepth) {
        // Get all URLs (links) from the current document.
        var urls = getAllLinks(document);

        for (var url : urls) {
            // Remove all hash marks from the URL.
            url = removeHashMarksFromURL(url);

            // If the URL has not been crawled yet, crawl it.
            if (!visitedUrls.contains(url)) {
                visitedUrls.add(url);
                processSite(url, currentDepth + 1);
            }
        }
    }

    /***
     * Checks if a URL is valid or not.
     * @param url inspected URL
     * @return true, if the URL is a valid URL. Otherwise, false.
     */
    private boolean isValidLink(String url) {
        try {
            new URI(url);
            return url.startsWith(rootUrl);
        } catch (URISyntaxException e) {
            return false;
        }
    }

    /***
     * Returns a list of all links found in the document.
     * @param document Document in which we went to find all links (other URLs)
     * @return list of find links
     */
    private List<String> getAllLinks(final Document document) {
        // Perform a CSS query to find all <a href="" /> elements.
        var links = document.select(HREF_CSS_QUERY);

        List<String> urls = new LinkedList<>();
        for (var link : links) {
            // Extract the URL from the link.
            String url = link.absUrl(HREF_ATTRIBUTE_KEY);

            // If it's a valid URL, add it to the list.
            if (isValidLink(url)) {
                urls.add(url);
            }
        }
        return urls;
    }

    /***
     * Prints data into a file on the disk. This method is used when creating
     * a dump file or when creating the final results of the crawl.
     * @param filename name of the file on the disk
     * @param data data to be printed out into the file
     */
    private void writeToFile(final String filename, final String data) {
        BufferedWriter bufferedWriter = null;
        try {
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally  {
            try {
                if (bufferedWriter != null)
                    bufferedWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /***
     * Creates a JSON object of the result of the crawl and stores
     * it into a file. Apart from storing all articles, it also stores
     * some crawl-related medata such as how many sites have been crawled,
     * timestamps, etc.
     * @param filename name of the file on the disk
     * @param timeStamp finish time of the crawl
     */
    private void storeCrawlResults(final String filename, final LocalDateTime timeStamp) {
        JSONObject jsonObj = new JSONObject();

        jsonObj.put("root_url", rootUrl);
        jsonObj.put("crawled_sites", visitedUrls.size());
        jsonObj.put("stated_at", getStartedCrawlingTime());
        jsonObj.put("finished_at", timeStamp);
        jsonObj.put("data", fetchedData);

        writeToFile(filename, jsonObj.toString());
    }

    /***
     * Creates a dump file containing all articles fetched down so far.
     */
    private void dumpCrawlResultSoFar() {
        var currentDateTime = LocalDateTime.now();
        int fetchedDataCount = fetchedData.length();
        storeCrawlResults(String.format(DUMP_FILENAME, fetchedDataCount, currentDateTime), currentDateTime);
        logger.info(String.format("created a dump of %d articles", fetchedDataCount));
    }

    /***
     * Returns a document to process. This method is called by the workers
     * when they want to parse (analyze) a document (website).
     * @return Optional of a document. If there isn't any document to be parsed, the
     *         optional is empty.
     */
    @Override
    public synchronized Optional<Document> getDocumentToProcess() {
        final var document = documentsToProcess.stream().findFirst();
        document.ifPresent(value -> documentsToProcess.remove(value));
        return document;
    }

    /***
     * Appends parsed data into the final list of all articles. This method is called
     * by the workers when they successfully manage to parse a website. They will
     * return a JSON object representing an article.
     * @param workerId id of the worker who has successfully parsed a document
     * @param data JSON object representing an article
     */
    @Override
    public synchronized void appendFetchedData(final int workerId, final JSONObject data) {
        fetchedData.put(data);
        logger.info(String.format("[#%d](worker: %d) has successfully processed %s", fetchedData.length(), workerId, data.get(URL_JSON_KEY)));
        if (fetchedData.length() % dumpLimit == 0) {
            dumpCrawlResultSoFar();
        }
    }

    /***
     * Starts the crawl.
     */
    @Override
    public void crawl() {
        initCrawl();
        startWorkers();
        processSite(rootUrl, 0);
        terminateCrawl();
        waitForWorkers();
        storeCrawlResults(FINAL_RESULTS_FILENAME, getFinishedCrawlingTime());
    }

    /***
     * Returns boolean value indicating whether the crawl has already finished or not.
     * @return true, if there are no more websites to crawl as well as no more documents
     *         waiting to be processed by one of the workers. False, otherwise.
     */
    @Override
    public boolean isFinished() {
        return crawlFinished && documentsToProcess.isEmpty();
    }

    /***
     * Returns the timestamp when the crawl started.
     * @return the start time of the crawl
     */
    @Override
    public LocalDateTime getStartedCrawlingTime() {
        return startedCrawling;
    }

    /***
     * Returns the timestamp when the crawl finished.
     * @return the finish time of the crawl
     */
    @Override
    public LocalDateTime getFinishedCrawlingTime() {
        return finishedCrawling;
    }
}
