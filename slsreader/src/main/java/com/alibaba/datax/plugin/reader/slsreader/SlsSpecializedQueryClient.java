//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.datax.plugin.reader.slsreader;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.QueriedLog;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.GetLogsResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlsSpecializedQueryClient {
    private static final Logger LOG = LoggerFactory.getLogger(SlsSpecializedQueryClient.class);
    private final String projectName;
    private final String logstoreName;
    private final String topic;
    private final String query;
    private final Client client;
    private final AtomicBoolean hasShutdown = new AtomicBoolean(false);

    public SlsSpecializedQueryClient(String host, String accessId, String accessKey, String projectName, String logstoreName, String topic, String query) {
        this.projectName = projectName;
        this.logstoreName = logstoreName;
        this.topic = topic;
        this.query = query;
        this.client = new Client(host, accessId, accessKey);
    }

    public List<QueriedLog> query(int fromTime, int toTime) {
        try {
            List<QueriedLog> queriedLogs = new ArrayList<>(10000);
            int start = 0;
            while (true) {
                if (start > 1000000) {
                    LOG.error("start:{} exceed max:1000000", start);
                    break;
                }
                GetLogsResponse logsResponse = this.client.GetLogs(this.projectName, this.logstoreName, fromTime, toTime, this.topic, this.query + " limit " + start + ",10000");
                LOG.info("sub query, start:{},count:{}", start, logsResponse.GetCount());
                if (logsResponse.GetCount() == 0) {
                    break;
                }
                queriedLogs.addAll(logsResponse.getLogs());
                start += 10000;
            }
            return queriedLogs;
        } catch (LogException var6) {
            LOG.error(var6.getMessage(), var6);
            return Collections.emptyList();
        }
    }

    public List<String> columnNames(int fromTime, int toTime) {
        String queryOne = this.query + " limit 0,1";

        try {
            GetLogsResponse logsResponse = this.client.GetLogs(this.projectName, this.logstoreName, fromTime, toTime, this.topic, queryOne);
            List<QueriedLog> logs = logsResponse.getLogs();
            if (logs != null && !logs.isEmpty()) {
                List<LogContent> contents = ((QueriedLog) logs.get(0)).mLogItem.mContents;
                return (List) contents.stream().map(LogContent::GetKey).collect(Collectors.toList());
            } else {
                return Collections.emptyList();
            }
        } catch (LogException var5) {
            LOG.error(var5.getMessage(), var5);
            return Collections.emptyList();
        }
    }

    public void shutdown() {
        if (this.hasShutdown.compareAndSet(false, true)) {
            this.client.shutdown();
        }

    }
}
