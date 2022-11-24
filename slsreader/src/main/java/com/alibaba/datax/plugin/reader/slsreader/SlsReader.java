package com.alibaba.datax.plugin.reader.slsreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.QueriedLog;
import org.apache.http.util.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SlsReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private SlsSpecializedQueryClient client;

        @Override
        public void init() {
            Configuration pluginJobConf = this.getPluginJobConf();
            String host = pluginJobConf.getString("host");
            Asserts.notBlank(host, "host");
            String accessId = pluginJobConf.getString("accessId");
            Asserts.notBlank(accessId, "accessId");
            String accessKey = pluginJobConf.getString("accessKey");
            Asserts.notBlank(accessKey, "accessKey");
            String projectName = pluginJobConf.getString("projectName");
            Asserts.notBlank(projectName, "projectName");
            String logstoreName = pluginJobConf.getString("logstoreName");
            Asserts.notBlank(logstoreName, "logstoreName");
            Integer fromTime = pluginJobConf.getInt("fromTime");
            Asserts.notNull(fromTime, "fromTime");
            Integer toTime = pluginJobConf.getInt("toTime");
            Asserts.notNull(toTime, "toTime");
            Asserts.check(fromTime < toTime, "fromTime must be less than toTime");
            String topic = pluginJobConf.getString("topic");
            if (topic == null) {
                topic = "";
                pluginJobConf.set("topic", topic);
            }
            Integer duration = pluginJobConf.getInt("duration");
            if (duration == null || duration <= 0) {
                duration = 3600;//seconds
                pluginJobConf.set("duration", duration);
            }
            String query = pluginJobConf.getString("query");
            Asserts.notNull(query, "query");

            List<String> columns = pluginJobConf.getList("column", String.class);
            if (columns == null || columns.isEmpty()) {
                throw new IllegalStateException("columns is null");
            }

            if (columns.stream().anyMatch(String::isEmpty)) {
                throw new IllegalStateException("there are column name that is empty");
            }

            client = new SlsSpecializedQueryClient(host, accessId, accessKey, projectName, logstoreName, topic, query);
            List<String> columnNames = client.columnNames(fromTime, toTime);
            LOG.info("column candidate:{}", columnNames);
            if (columnNames.isEmpty()) {
                throw new IllegalArgumentException("column names is empty, it may caused by query:" + query + ", please check query parameter");
            }
            Set<String> columnSet = new HashSet<>(columnNames);
            for (String col : columns) {
                if (!columnSet.contains(col)) {
                    throw new IllegalStateException("column :" + col + " not in candidate set:" + String.join(",", columnSet));
                }
            }

            Map<String, Integer> columnIndex = new HashMap<>(16);
            for (int i = 0; i < columns.size(); i++) {
                columnIndex.put(columns.get(i), i);
            }
            pluginJobConf.set("columnIndex", columnIndex);
        }

        @Override
        public void destroy() {
            LOG.info("sls job destroy");
            client.shutdown();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            Configuration pluginJobConf = this.getPluginJobConf();
            int concurrent = pluginJobConf.getInt("concurrent");
            int fromTime = pluginJobConf.getInt("fromTime");
            int toTime = pluginJobConf.getInt("toTime");
            int duration = pluginJobConf.getInt("duration");

            //split (fromTime,duration) into n piece by duration
            int start = fromTime;
            int end = 0;
            List<TimeBoundary> boundaries = new ArrayList<>(32);
            while (start < toTime) {
                end = start + duration;
                if (end > toTime) {
                    end = toTime;
                }
                TimeBoundary timeBoundary = new TimeBoundary();
                timeBoundary.setFrom(start);
                timeBoundary.setTo(end);
                boundaries.add(timeBoundary);
                start += duration;
            }

            List<Map<String, String>> boundaryList = new ArrayList<>(concurrent);
            for (int i = 0; i < concurrent; i++) {
                boundaryList.add(new HashMap<>());
            }
            for (int i = 0; i < boundaries.size(); i++) {
                TimeBoundary timeBoundary = boundaries.get(i);
                boundaryList.get(i % concurrent).put(timeBoundary.getFrom() + "", timeBoundary.getTo() + "");
            }
            List<Configuration> configurations = new ArrayList<>(concurrent);
            for (int i = 0; i < concurrent; i++) {
                Configuration configuration = pluginJobConf.clone();
                configuration.set("timeBoundary", boundaryList.get(i));
                configurations.add(configuration);
            }
            return configurations;
        }
    }

    public static class Task extends Reader.Task {

        private static final AtomicInteger TASK_COUNTER = new AtomicInteger(0);

        private static final Logger LOG = LoggerFactory
                .getLogger(SlsSpecializedQueryClient.class);

        private Map<String, String> timeBoundary;

        private Map<String, Integer> columnIndex;

        private int columnCount;

        private int no = TASK_COUNTER.getAndIncrement();

        private SlsSpecializedQueryClient client;

        @Override
        public void init() {
            Configuration pluginJobConf = this.getPluginJobConf();
            String host = pluginJobConf.getString("host");
            String accessId = pluginJobConf.getString("accessId");
            String accessKey = pluginJobConf.getString("accessKey");
            String projectName = pluginJobConf.getString("projectName");
            String logstoreName = pluginJobConf.getString("logstoreName");
            String topic = pluginJobConf.getString("topic");
            String query = pluginJobConf.getString("query");
            timeBoundary = (Map<String, String>) pluginJobConf.get("timeBoundary", Map.class);
            this.columnIndex = pluginJobConf.getMap("columnIndex", Integer.class);
            LOG.info("columnIndex:{}", columnIndex);
            this.columnCount = columnIndex.keySet().size();
            LOG.info("configuration:{}", pluginJobConf);
            client = new SlsSpecializedQueryClient(host, accessId, accessKey, projectName, logstoreName, topic, query);
        }

        @Override
        public void destroy() {
            client.shutdown();
            client = null;
            LOG.info("destroy task, No: {}", no);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            for (Map.Entry<String, String> bound : timeBoundary.entrySet()) {
                List<QueriedLog> queriedLogs = client.query(Integer.parseInt(bound.getKey()), Integer.parseInt(bound.getValue()));
                LOG.info("Task No: {}, fromTime:{}, toTime:{}, log count:{}", no, bound.getKey(), bound.getValue(), queriedLogs.size());
                for (QueriedLog log : queriedLogs) {
                    Record record = recordSender.createRecord();
                    fillRecord(record, log.mLogItem);
                    recordSender.sendToWriter(record);
                }
            }
        }

        private void fillRecord(Record record, LogItem logItem) {
            StringColumn[] stringColumns = new StringColumn[this.columnCount];
            for (LogContent content : logItem.mContents) {
                String key = content.GetKey();
                Integer index = columnIndex.get(key);
                if (index == null) {
                    continue;
                }
                stringColumns[index] = new StringColumn(content.GetValue());
            }
            Arrays.stream(stringColumns).forEach(record::addColumn);
        }
    }


    private static class TimeBoundary {
        int from;

        int to;

        public int getFrom() {
            return from;
        }

        public void setFrom(int from) {
            this.from = from;
        }

        public int getTo() {
            return to;
        }

        public void setTo(int to) {
            this.to = to;
        }
    }
}
