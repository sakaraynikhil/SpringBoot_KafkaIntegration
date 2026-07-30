package receive.ReceiveService.EventHandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import receive.ReceiveService.Entity.Product;
import receive.ReceiveService.Events.ProductEvent;
import receive.ReceiveService.Respository.ProductRepository;

@Component
@KafkaListener(topics = "product-test-topic")
@Slf4j
public class ProductEventHandler {
    private static final Logger log = LogManager.getLogger(ProductEventHandler.class);

    //injecting the dependencies

    private ProductRepository productRepository;

    @Autowired
    public ProductEventHandler(ProductRepository productRepository){
        this.productRepository = productRepository;
    }


    @KafkaHandler
    public void handleSaveTopic(ProductEvent productEvent){

        Product product = new Product();

        product.setId(productEvent.getId());
        product.setProduct_name(productEvent.getProduct_name());
        product.setQuantity(productEvent.getQuantity());

        productRepository.save(product);

        log.info("Product details are saved "+ "Product Name :"+productEvent.getProduct_name());

    }

}
