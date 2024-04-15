/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * File chunks are sent/requested sequentially by at most one thread at any time. However, the sender/requestor won't wait for the response
 * before processing the next file chunk request to reduce the recovery time especially on secure/compressed or high latency communication.
 * <p>
 * The sender/requestor can send up to {@code maxConcurrentFileChunks} file chunk requests without waiting for responses. Since the recovery
 * target can receive file chunks out of order, it has to buffer those file chunks in memory and only flush to disk when there's no gap.
 * To ensure the recover target never buffers more than {@code maxConcurrentFileChunks} file chunks, we allow the sender/requestor to send
 * only up to {@code maxConcurrentFileChunks} file chunk requests from the last flushed (and acknowledged) file chunk. We leverage the local
 * checkpoint tracker for this purpose. We generate a new sequence number and assign it to each file chunk request before sending; then mark
 * that sequence number as processed when we receive a response for the corresponding file chunk request. With the local checkpoint tracker,
 * we know the last acknowledged-flushed file-chunk is a file chunk whose {@code requestSeqId} equals to the local checkpoint because the
 * recover target can flush all file chunks up to the local checkpoint.
 * <p>
 * When the number of un-replied file chunk requests reaches the limit (i.e. the gap between the max_seq_no and the local checkpoint is
 * greater than {@code maxConcurrentFileChunks}), the sending/requesting thread will abort its execution. That process will be resumed by
 * one of the networking threads which receive/handle the responses of the current pending file chunk requests. This process will continue
 * until all chunk requests are sent/responded.
 * 文件块在任何时候最多由一个线程按顺序发送/请求。然而，发送方/请求方不会在处理下一个文件块请求之前等待响应，以减少恢复时间，尤其是在安全/压缩或高延迟通信中。
 * 发送方/请求方最多可以发送maxConcurrentFileChunks文件块请求，而无需等待响应。由于恢复目标可能会无序地接收文件块，因此它必须在内存中缓冲这些文件块，
 * * 并且只有在没有间隙时才刷新到磁盘。为了确保恢复目标的缓冲区永远不会超过maxConcurrentFileChunks文件块，
 * * 我们允许发送方/请求方从最后一个刷新（和确认）的文件块中只发送最多maxConcurrentFile chunks的文件块请求。
 * * 为此，我们利用本地检查点跟踪器。我们生成一个新的序列号，并在发送之前将其分配给每个文件块请求；然后当我们接收到对应文件块请求的响应时，
 * * 将该序列号标记为已处理。使用本地检查点跟踪器，我们知道最后确认的刷新文件块是requestSeqId等于本地检查点的文件块，因为恢复目标可以将所有文件块刷新到本地检查点。
 * 当未回复的文件块请求数量达到限制时（即，max_seq_no和本地检查点之间的间隙大于maxConcurrentFileChunks），
 * * 发送/请求线程将中止其执行。该过程将由接收/处理当前挂起的文件块请求的响应的网络线程之一恢复。此过程将继续，直到发送/响应所有区块请求为止。
 */
public abstract class MultiChunkTransfer<Source, Request extends MultiChunkTransfer.ChunkRequest> implements Closeable {
    private Status status = Status.PROCESSING;
    private final Logger logger;
    private final ActionListener<Void> listener;
    private final LocalCheckpointTracker requestSeqIdTracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final AsyncIOProcessor<FileChunkResponseItem<Source>> processor;
    private final int maxConcurrentChunks;
    private Source currentSource = null;
    private final Iterator<Source> remainingSources;
    private Tuple<Source, Request> readAheadRequest = null;

    protected MultiChunkTransfer(Logger logger, ThreadContext threadContext, ActionListener<Void> listener,
                                 int maxConcurrentChunks, List<Source> sources) {
        this.logger = logger;
        this.maxConcurrentChunks = maxConcurrentChunks;
        this.listener = listener;
        this.processor = new AsyncIOProcessor<FileChunkResponseItem<Source>>(logger, maxConcurrentChunks, threadContext) {
            @Override
            protected void write(List<Tuple<FileChunkResponseItem<Source>, Consumer<Exception>>> items) throws IOException {
                handleItems(items);
            }
        };
        this.remainingSources = sources.iterator();
    }

    public final void start() {
        addItem(UNASSIGNED_SEQ_NO, null, null); // put a dummy item to start the processor
    }

    private void addItem(long requestSeqId, Source resource, Exception failure) {
        processor.put(new FileChunkResponseItem<>(requestSeqId, resource, failure), e -> { assert e == null : e; });
    }

    private void handleItems(List<Tuple<FileChunkResponseItem<Source>, Consumer<Exception>>> items) {
        if (status != Status.PROCESSING) {
            assert status == Status.FAILED : "must not receive any response after the transfer was completed";
            // These exceptions will be ignored as we record only the first failure, log them for debugging purpose.
            items.stream().filter(item -> item.v1().failure != null).forEach(item ->
                logger.debug(new ParameterizedMessage("failed to transfer a chunk request {}", item.v1().source), item.v1().failure));
            return;
        }
        try {
            for (Tuple<FileChunkResponseItem<Source>, Consumer<Exception>> item : items) {
                final FileChunkResponseItem<Source> resp = item.v1();
                if (resp.requestSeqId == UNASSIGNED_SEQ_NO) {
                    continue; // not an actual item
                }
                requestSeqIdTracker.markSeqNoAsProcessed(resp.requestSeqId);
                if (resp.failure != null) {
                    handleError(resp.source, resp.failure);
                    throw resp.failure;
                }
            }
            while (requestSeqIdTracker.getMaxSeqNo() - requestSeqIdTracker.getProcessedCheckpoint() < maxConcurrentChunks) {
                final Tuple<Source, Request> request = readAheadRequest != null ? readAheadRequest : getNextRequest();
                readAheadRequest = null;
                if (request == null) {
                    assert currentSource == null && remainingSources.hasNext() == false;
                    if (requestSeqIdTracker.getMaxSeqNo() == requestSeqIdTracker.getProcessedCheckpoint()) {
                        onCompleted(null);
                    }
                    return;
                }
                final long requestSeqId = requestSeqIdTracker.generateSeqNo();
                //request这个tuple的第一个元素是StoreFileMetadata，第二个元素是segment中读到的文件内容chunk块
                executeChunkRequest(request.v2(), ActionListener.wrap(
                    r -> addItem(requestSeqId, request.v1(), null),
                    e -> addItem(requestSeqId, request.v1(), e)));
            }
            // While we are waiting for the responses, we can prepare the next request in advance
            // so we can send it immediately when the responses arrive to reduce the transfer time.
            if (readAheadRequest == null) {
                readAheadRequest = getNextRequest();
            }
        } catch (Exception e) {
            onCompleted(e);
        }
    }

    private void onCompleted(Exception failure) {
        if (Assertions.ENABLED && status != Status.PROCESSING) {
            throw new AssertionError("invalid status: expected [" + Status.PROCESSING + "] actual [" + status + "]", failure);
        }
        status = failure == null ? Status.SUCCESS : Status.FAILED;
        try {
            IOUtils.close(failure, this);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(null);
    }

    private Tuple<Source, Request> getNextRequest() throws Exception {
        try {
            if (currentSource == null) {
                if (remainingSources.hasNext()) {
                    currentSource = remainingSources.next();//获取下一个数据目录
                    onNewResource(currentSource);
                } else {
                    return null;
                }
            }
            final Source md = currentSource;
            final Request request = nextChunkRequest(md);//获取文件chunk
            if (request.lastChunk()) {
                currentSource = null;
            }
            return Tuple.tuple(md, request);
        } catch (Exception e) {
            handleError(currentSource, e);
            throw e;
        }
    }

    /**
     * This method is called when starting sending/requesting a new source. Subclasses should override
     * this method to reset the file offset or close the previous file and open a new file if needed.
     */
    protected void onNewResource(Source resource) throws IOException {

    }

    protected abstract Request nextChunkRequest(Source resource) throws IOException;

    protected abstract void executeChunkRequest(Request request, ActionListener<Void> listener);

    protected abstract void handleError(Source resource, Exception e) throws Exception;

    private static class FileChunkResponseItem<Source> {
        final long requestSeqId;
        final Source source;
        final Exception failure;

        FileChunkResponseItem(long requestSeqId, Source source, Exception failure) {
            this.requestSeqId = requestSeqId;
            this.source = source;
            this.failure = failure;
        }
    }

    public interface ChunkRequest {
        /**
         * @return {@code true} if this chunk request is the last chunk of the current file
         */
        boolean lastChunk();
    }

    private enum Status {
        PROCESSING,
        SUCCESS,
        FAILED
    }
}
