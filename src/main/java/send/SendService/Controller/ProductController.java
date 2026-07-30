package send.SendService.Controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import send.SendService.Events.ProductEvent;
import send.SendService.Service.ProductService;

@RestController
@RequestMapping("/api/product")
public class ProductController {

    //injecting the necessary dependencies
    private ProductService productService;

    @Autowired
    public ProductController(ProductService productService){
        this.productService = productService;
    }


    @PostMapping("/send")
    public ResponseEntity<String> sendEvent(@RequestBody ProductEvent productEvent){
       try{
           productService.saveProduct(productEvent);
           return ResponseEntity
                   .status(HttpStatus.OK)
                   .body("The message has been successfully sent");
       }catch(Exception e){
           System.out.println("handling the exception");
       }
       return ResponseEntity
               .status(HttpStatus.INTERNAL_SERVER_ERROR)
               .body("Message Failed to send");


    }





}
