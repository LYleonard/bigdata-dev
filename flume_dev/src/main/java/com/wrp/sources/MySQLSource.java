package com.wrp.sources;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName MySQLSource
 * @Author LYleonard
 * @Date 2020/4/4 22:13
 * @Description 自定义source实现从mysql中获取数据
 * Version 1.0
 **/
public class MySQLSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSource.class);

    private QueryMysql sqlSourceHelper;

    /**
     * 接受mysql表中的数据
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        try {
            //查询数据
            List<List<Object>> result = sqlSourceHelper.executeQuery();
            //存放event的集合
            List<Event> events = new ArrayList<>();
            //存放event头集合
            HashMap<String, String> header = new HashMap<>();
            //如果返回数据，则将数据封装为event
            if (!result.isEmpty()) {
                List<String> allRows = sqlSourceHelper.getAllRows(result);
                Event event = null;
                for (String row : allRows) {
                    event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(header);
                    events.add(event);
                }
                // 将event写入channel
                this.getChannelProcessor().processEventBatch(events);
                // 更新数据表中的offset信息
                sqlSourceHelper.updateOffset2DB(result.size());
            }
            // 等待时长

            Thread.sleep(sqlSourceHelper.getRunQueryDelay());
            return Status.READY;
        } catch (InterruptedException e) {
            logger.error("Error processing row ", e);
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        try {
            sqlSourceHelper = new QueryMysql(context);
        } catch (ParseException | ConfigurationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping sql source {} ...", getName());
        try {
            // 关闭资源
            sqlSourceHelper.close();
        } finally {
            super.stop();
        }

    }
}
