package org.eminem.hbase.observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bulk hbase data to ElasticSearch Class
 */
public class ElasticSearchBulkOperator {

    private static final Log LOG = LogFactory.getLog(ElasticSearchBulkOperator.class);
    //此处设置批量提交数量
    private static final int MAX_BULK_COUNT = 5000;

    private BulkRequestBuilder bulkRequestBuilder = null;

    private Lock commitLock = new ReentrantLock();

    private ScheduledExecutorService scheduledExecutorService = null;
    private EsClient esClient = null;
    public ElasticSearchBulkOperator(final EsClient esClient) {
        LOG.info("----------------- Init Bulk Operator for Table: " + " ----------------");
        this.esClient = esClient;
        // init es bulkRequestBuilder
        this.bulkRequestBuilder = esClient.getClient().prepareBulk();
        // init thread pool and set size 1
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

        // create beeper thread( it will be sync data to ES cluster)use a commitLock to protected bulk es as thread-save
        Runnable beeper = new Runnable() {
            @Override
            public void run() {
                commitLock.lock();
                try {
                    //LOG.info("Scheduled Thread start run for ");
                    bulkRequest(0);
                } catch (Exception ex) {
                    LOG.error("Time Bulk " + " index error : " + ex.getMessage());
                } finally {
                    commitLock.unlock();
                }
            }
        };

        // set beeper thread(15 second to delay first execution , 25 second period between successive executions)
        scheduledExecutorService.scheduleAtFixedRate(beeper, 15, 25, TimeUnit.SECONDS);

    }

    /**
     * shutdown time task immediately
     */
    public void shutdownScheduEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * bulk request when number of builders is grate then threshold
     *
     * @param threshold
     */
    public void bulkRequest(int threshold) {
        int count = bulkRequestBuilder.numberOfActions();
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            try {
                LOG.info("Bulk Request Run " + ", the row count is: " + count);
                BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
                if (bulkItemResponse.hasFailures()) {
                    LOG.error("------------- Begin: Error Response Items of Bulk Requests to ES ------------");
                    LOG.error(bulkItemResponse.buildFailureMessage());
                    LOG.error("------------- End: Error Response Items of Bulk Requests to ES ------------");
                }
                bulkRequestBuilder = esClient.getClient().prepareBulk();
            } catch (Exception e) {// two cause: 1. transport client is closed 2. None of the configured nodes are available
                LOG.error(" Bulk Request " + " index error : " + e.getMessage());
                LOG.error("Reconnect the ES server...");
                List<DocWriteRequest> tempRequests = bulkRequestBuilder.request().requests();
                esClient.getClient().close();
                esClient.repeatInitEsClient();
                bulkRequestBuilder = esClient.getClient().prepareBulk();
                bulkRequestBuilder.request().add(tempRequests);
            }
        }
    }

    /**
     * add update builder to bulk use commitLock to protected bulk as
     * thread-save
     *
     * @param builder
     */
    public void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" Add Bulk index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * add delete builder to bulk use commitLock to protected bulk as
     * thread-save
     *
     * @param builder
     */
    public void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception ex) {
            LOG.error(" delete Bulk index error : " + ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }
}
