package os.kai.mq.server.controller;

import os.kai.mq.server.service.MessageQueueBrokerService;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/mq")
public class MessageQueueBrokerController {
    private static final String P_SUBJECT = "subject";
    private static final String P_GROUP = "group";
    private static final String OK = "ok";

    private static final long DEF_TIMEOUT = 30L*1000L;

    private final MessageQueueBrokerService service;

    public MessageQueueBrokerController(MessageQueueBrokerService service) {
        this.service = service;
    }

    @PostMapping("/publish/{"+P_SUBJECT+"}")
    public String publish(
            @PathVariable("subject") String subject,
            @RequestBody List<String> values
    ) {
        service.put(subject,values);
        return OK;
    }

    @GetMapping("/poll/{"+P_SUBJECT+"}/{"+P_GROUP+"}")
    public List<String> poll(
            @PathVariable(P_SUBJECT) String subject,
            @PathVariable(P_GROUP) String group,
            @RequestParam @Nullable String timeoutStr,
            @RequestParam @Nullable String maxCountStr
    ) {
        long timeout;
        try{
            timeout = Long.parseLong(timeoutStr);
        }
        catch(Exception e){
            timeout = DEF_TIMEOUT;
        }
        int maxCount;
        try{
            maxCount = Integer.parseInt(maxCountStr);
        }
        catch(Exception e){
            maxCount = 1;
        }
        try{
            return service.take(subject,group,timeout,maxCount);
        }catch(InterruptedException e){
            return Collections.emptyList();
        }
    }
}
