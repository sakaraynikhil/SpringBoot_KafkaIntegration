package send.SendService.Service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import send.SendService.Events.ProductEvent;

import java.util.UUID;

@Service
public class ServiceImpl implements ProductService{


    private KafkaTemplate<String,ProductEvent> kafkaTemplate;

    @Autowired
    public ServiceImpl(KafkaTemplate<String,ProductEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String saveProduct(ProductEvent productEvent) throws Exception {

        ProductEvent prodEvent = new ProductEvent();

        prodEvent.setId(productEvent.getId());
        prodEvent.setProduct_name(productEvent.getProduct_name());
        prodEvent.setQuantity(productEvent.getQuantity());

        String producerId = UUID.randomUUID().toString();

        SendResult<String,ProductEvent> result = kafkaTemplate.send("product-test-topic",producerId,prodEvent).get();

        return "The product has been successfully sent";
    }

    @Override
    public ProductEvent getProductDetails(int id) {
        ProductEvent productEvent = new ProductEvent();
        

        return productEvent;
    }


}
