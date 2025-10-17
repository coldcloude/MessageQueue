package os.kai.mq.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageQueueBroker {

    static class MQ {
        final SingleQueue<String> queue = new SingleQueue<>();
        final ConcurrentLinkedQueue<Long> checks = new ConcurrentLinkedQueue<>();
    }

    private final long checkInterval;
    private final long globalMaxTimeout;

    private final Map<String,Long> maxTimeoutMap = new ConcurrentHashMap<>();
    private final Map<String,Boolean> keepPolicyMap = new ConcurrentHashMap<>();

    private final Map<String,MQ> mqMap = new ConcurrentHashMap<>();

    class Check extends TimerTask {
        @Override
        public void run() {
            for(Map.Entry<String,MQ> entry : mqMap.entrySet()){
                String subject = entry.getKey();
                MQ mq = entry.getValue();
                //save current index
                long index = mq.queue.getMaxIndex();
                mq.checks.offer(index);
                //remove when not keep
                boolean keep = keepPolicyMap.getOrDefault(subject,false);
                if(!keep){
                    mq.queue.clean();
                }
                //remove timeout
                long maxTimeout = maxTimeoutMap.getOrDefault(subject,globalMaxTimeout);
                int maxSize = (int)Math.ceil((double)maxTimeout/(double)checkInterval);
                Long minIndex;
                while(mq.checks.size()>maxSize&&(minIndex=mq.checks.poll())!=null){
                    mq.queue.clean(minIndex);
                }
            }
        }
    }

    public MessageQueueBroker(long checkInterval,long globalMaxTimeout){
        this.checkInterval = checkInterval;
        this.globalMaxTimeout = globalMaxTimeout;
        new Timer().schedule(new Check(),checkInterval);
    }

    public void setMaxTimeout(String subject, long timeout){
        maxTimeoutMap.put(subject,timeout);
    }

    public void resetMaxTimeout(String subject){
        maxTimeoutMap.remove(subject);
    }

    public void keep(String subject){
        keepPolicyMap.put(subject,true);
    }

    public void unkeep(String subject){
        keepPolicyMap.remove(subject);
    }

    private SingleQueue<String> getQueue(String subject){
        MQ mq = mqMap.computeIfAbsent(subject,k->new MQ());
        return mq.queue;
    }

    private SingleQueue<String>.Consumer getConsumer(String subject, String group, boolean earliest){
        return getQueue(subject).getOrCreateConsumer(group,earliest);
    }

    public String take(String subject, String group, long timeout, boolean earliest) throws InterruptedException {
        return getConsumer(subject,group,earliest).take(timeout);
    }
    public String take(String subject, String group, long timeout) throws InterruptedException {
        return take(subject,group,timeout,false);
    }

    public List<String> take(String subject, String group, long timeout, int maxCount, boolean earliest) throws InterruptedException {
        return getConsumer(subject,group,earliest).take(timeout,maxCount);
    }

    public List<String> take(String subject,String group,long timeout,int maxCount) throws InterruptedException {
        return take(subject,group,timeout,maxCount,false);
    }

    public void put(String subject, Iterable<String> values){
        getQueue(subject).put(values);
    }
}
