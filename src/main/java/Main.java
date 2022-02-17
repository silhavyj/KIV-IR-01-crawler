import crawler.IWebCrawler;
import crawler.WebCrawler;
import processor.BBCNewsProcessor;
import processor.ISiteProcessor;

import java.util.LinkedList;

/***
 * @author silhavyj
 *
 * This class represets the Main class of the project.
 * Its main purpose is to start the crawler.
 */
public class Main {

    /*** Main (root) URL from which the crawler starts. */
    private static final String ROOT_URL = "https://www.bbc.com/news";

    /*** Maximum depth at which the crawler goes. */
    private static final int MAX_DEPTH = 10;

    /*** How often a dump of all articles will be created, e.g. every 100 articles. */
    private static final int DUMP_PERIOD = 200;

    /*** Total number of workers to be created. */
    private static final int NUMBER_OF_WORKERS = 10;

    /***
     * Creates a list of workers that periodically ask the crawler for a document to process.
     * @return collection of all workers
     */
    private static LinkedList<ISiteProcessor> createSiteProcessors() {
        final var siteProcessors = new LinkedList<ISiteProcessor>();
        for (int i = 0; i < Main.NUMBER_OF_WORKERS; i++) {
            siteProcessors.add(new BBCNewsProcessor());
        }
        return siteProcessors;
    }

    /***
     * This is the main entry point of the application.
     * @param args parameters passed in from the terminal (unused)
     */
    public static void main(String[] args) {
        // Create workers (list of site processors).
        final var siteProcessors = createSiteProcessors();

        // Create an instance of IWebCrawler and start crawling the site.
        final IWebCrawler webCrawler = new WebCrawler(ROOT_URL, MAX_DEPTH, DUMP_PERIOD, siteProcessors);
        webCrawler.crawl();
    }
}
