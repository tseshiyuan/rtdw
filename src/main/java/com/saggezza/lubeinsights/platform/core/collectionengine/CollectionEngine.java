package com.saggezza.lubeinsights.platform.core.collectionengine;

import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleFactory;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;

/**
 * Created by chiyao on 8/25/14.
 */

/**
 * CollectionEngine is designed to work with CollectionSources.
 * There are two set up paradigms:
 * 1. Each CollectionSource is a kafka consumer that gets data from a specific topic represented by an end point collector.
 *    Each batch from a source is a stream of data records between a begin-batch token and an end-batch token, populated by end point collector.
 *    End-point collector and CollectionSource has a one-to-one mapping such that record order can be reserved.
 * 2. Each CollectionSource a kafka consumer that gets data from a specific topic representing a live stream
 *    Live streams are collected without batching or one-to-one mapping
 */
public class CollectionEngine  extends PlatformService {

    public static final Logger logger = Logger.getLogger(CollectionEngine.class);

    public static final int CHECK_POINT_RECS = 1024;

    protected HashMap<String, StreamCollectionSource> allStreamSources = new HashMap<String, StreamCollectionSource>();

    public CollectionEngine() {
        super(ServiceName.COLLECTION_ENGINE);
    }

    public ServiceResponse processRequest(ServiceRequest request, String command) {
        try {

            logger.info("CollectionEngine processes request:\n" + request.toJson());

            if (request == null) {
                logger.error("request is null");
            }

            ArrayList<ServiceRequest.ServiceStep> steps = request.getCommandList();

            if (steps == null || steps.size() != 1) {
                logger.error("Bad Request for CollectionEngine. Only one command is allowed in request");
                return new ServiceResponse("ERROR", "Bad Request for CollectionEngine. Only one command is allowed in request", null);
            }

            ServiceRequest.ServiceStep step = steps.get(0);
            Params params = step.getParams();

            // command interpreter
            ServiceResponse response = null;
            switch (step.getCommand()) {
                case COLLECT_BATCH:
                    // parse params

                    String sourceDesc = params.getValue("sourceDesc");
                    DataModel dataModel = params.getValue("dataModel");
                    DataRef dataRef = params.getValue("dataRef");
                    String batchId = params.getValue("batchId");
                    String parser = params.getValue("parser");
                    Boolean cleanup = params.getValue("cleanup");
                    // call method
                    logger.info("collectBatch "+batchId+ " from source "+sourceDesc);
                    collectBatch(sourceDesc,dataModel,dataRef,batchId,parser,cleanup);
                    logger.info("collectBatch done");
                   // build response
                    response = new ServiceResponse("OK", null, null);
                    break;

                case START_COLLECTING_STREAM:
                    // parse params
                    sourceDesc = params.getValue("sourceDesc");
                    dataModel = params.getValue("dataModel");
                    dataRef = params.getValue("dataRef");
                    parser = params.getValue("parser");
                    String groupId = params.getValue("groupId");
                    // call method
                    startCollectingStream(sourceDesc,dataModel,dataRef,parser,groupId);
                    // build response
                    response = new ServiceResponse("OK", null, null);
                    break;

                case STOP_COLLECTING_STREAM:
                    // parse params
                    sourceDesc = params.getValue("sourceDesc");
                    // call method
                    stopCollectingStream(sourceDesc);
                    // build response
                    response = new ServiceResponse("OK", null, null);
                    break;

                default:
                    // only support one command now
                    response = new ServiceResponse("ERROR", "Bad command for CollectionEngine.", null);
            }
            logger.info("CollectionEngine's response:\n" + response.toJson());
            return response;
        } catch (Exception e) {
            logger.trace("CollectionEngine Error", e);
            logger.error("CollectionEngine Error "+ e.getMessage());
            e.printStackTrace();
            return new ServiceResponse("CollectionEngine ERROR", e.getMessage(), null);
        }
    }


    /**
     * collect a batch with id batchId from the source
     * If parser is not null, parse it with dataModel into DataElement
     * This is a sync call
     *
     * @param sourceDesc
     * @param dataModel
     * @param dataRef
     * @param batchId
     * @param parser
     * @throws RuntimeException
     */
    public void collectBatch(String sourceDesc, DataModel dataModel, DataRef dataRef, String batchId, String parser, boolean cleanup) throws RuntimeException {

        try (BatchCollectionSource source = new BatchCollectionSource(sourceDesc)) {  // source will auto close upon finishing

            if (dataRef.getType() != DataRefType.FILE) {
                throw new RuntimeException("collect() can only handle DataRef of type FILE");
            }

            LinkedBlockingQueue<String> recsInBatch = source.getBatch(batchId);
            if (recsInBatch == null) {
                logger.warn("CollectionEngine cannot collect unavailable batch "+batchId);
                return; // batch is not available (topic does not exist)
            }

            try (PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(dataRef.getFileName())))) {

                BiFunction<String, DataModel, DataElement> parserFunc = null;
                if (parser != null) {
                    parserFunc = (BiFunction<String, DataModel, DataElement>)
                            ModuleFactory.getModule("parser", parser);
                }

                // block until data available
                // read until EndOfBatch (This call also blocks)
                for (String rec = recsInBatch.take(); !rec.equals(KafkaUtil.EOB); rec = recsInBatch.take()) {
                    // make sure it parses this model
                    if (parser != null) {
                        DataElement element = parserFunc.apply(rec, dataModel);  // optional
                        // save it to DataRef
                        // TODO: use DataStore instead of DataRef
                        output.println(element.toString());
                    } else {
                        output.println(rec);
                        System.out.println(rec);
                    }
                }
                source.doneBatch(batchId, cleanup); // commit Kafka offset and possibly clean up collection source

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e); // don't handle it, just convert to RuntimeException
            }
        }
    }



    /**
     * Start collecting the data stream from source
     * If parser is not null, parse it with dataModel into DataElement
     *
     * @param sourceDesc
     * @param dataModel
     * @param dataRef
     * @param parser
     * @throws RuntimeException
     */
    public void startCollectingStream(String sourceDesc, DataModel dataModel, DataRef dataRef, String parser, String groupId) throws RuntimeException {

        StreamCollectionSource source = allStreamSources.get(sourceDesc);
        if (source == null) {
            source = new StreamCollectionSource(sourceDesc, groupId); // sourceDesc determines groupId
            allStreamSources.put(sourceDesc, source);
            source.start();
        }

        if (dataRef.getType() != DataRefType.FILE) {
            throw new RuntimeException("collect() can only handle DataRef of type FILE");
        }

        ExecutorService executor = Executors.newFixedThreadPool(1);
        final StreamCollectionSource collectionSource = source;
        Runnable job = new Runnable() {
            @Override
            public void run() {
                // todo: use DataStore
                try (PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(dataRef.getFileName())))) {
                    BiFunction<String, DataModel, DataElement> parserFunc = null;
                    if (parser != null) {
                        parserFunc = (BiFunction<String, DataModel, DataElement>)
                                ModuleFactory.getModule("parser", parser);
                    }

                    while (true) {
                        int count=0;
                        for (String rec = collectionSource.nextRec(); rec != null && !collectionSource.stopSignaled(); rec = collectionSource.nextRec()) {
                            // make sure it parses this model
                            if (parser != null) {
                                DataElement element = parserFunc.apply(rec, dataModel);  // optional
                                // save it to DataRef
                                output.write(element.toString());
                            } else {
                                output.write(rec);
                                System.out.println(rec);
                            }
                            // check point every CHECK_POINT_RECS records
                            count = (count+1)%CHECK_POINT_RECS;
                            if (count==0) {
                                collectionSource.commit();
                            }
                        }
                        if (collectionSource.stopSignaled()) {
                            break;
                        }
                        Thread.sleep(60000); // rest 1 minute
                    }
                    // shut down the source
                    collectionSource.close();
                    // remove me from source pool after shut down
                    allStreamSources.remove(collectionSource.desc);

                } catch (Exception e) {
                    throw new RuntimeException(e); // don't handle it, just convert to RuntimeException
                }
            } // run
        }; // job
        executor.submit(job);
    }

    /**
     * signal to stop collecting this stream from the source of sourceDesc
     * @param sourceDesc
     * @throws RuntimeException
     */
    public void stopCollectingStream(String sourceDesc) throws RuntimeException {
        StreamCollectionSource source = allStreamSources.get(sourceDesc);
        if (source != null) {
            source.close(); // signal to stop
        }
    }

    public static final void main(String[] args) {
        /*
        CollectionEngine collectionEngine = new CollectionEngine();
        DataRef result = new DataRef(DataRefType.FILE,args[0]);
        collectionEngine.collectBatch("FileCollector.activity",null,result,"activity-log.1",null,false);
        */

        // test functionality
        Params params = Params.ofPairs(
                "sourceDesc","FileCollector.myCollector",
                "dataModel",null,
                "dataRef", new DataRef(DataRefType.FILE,args[0]), // specify destination as program argument
                "batchId", "activity-log",
                "parser",null,
                "cleanup",Boolean.FALSE
                );
        ServiceRequest request = new ServiceRequest(ServiceCommand.COLLECT_BATCH,params);
        CollectionEngine collectionEngine = new CollectionEngine();
        ServiceResponse response = collectionEngine.processRequest(request, null);
        System.out.println("status: "+response.getStatus());
        System.out.println("message: "+response.getMessage());
        System.out.println("data: "+response.getData());




    }

}
