package os.kai.mq.server.service;

import os.kai.mq.core.MessageQueueBroker;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MessageQueueBrokerService {
    private final long CHECK_INTERVAL = 60L*1000L;
    private final long MAX_TIMEOUT = 24L*60L*CHECK_INTERVAL;

    private final MessageQueueBroker broker = new MessageQueueBroker(CHECK_INTERVAL,MAX_TIMEOUT);


    public void setMaxTimeout(String subject, long timeout){
        broker.setMaxTimeout(subject,timeout);
    }

    public String take(String subject, String group, long timeout) throws InterruptedException {
        return broker.take(subject,group,timeout);
    }

    public List<String> take(String subject,String group,long timeout,int maxCount) throws InterruptedException {
        return broker.take(subject,group,timeout,maxCount);
    }

    public void put(String subject, Iterable<String> values){
        broker.put(subject,values);
    }
}
