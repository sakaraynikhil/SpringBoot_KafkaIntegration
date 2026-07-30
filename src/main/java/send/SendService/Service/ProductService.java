package send.SendService.Service;

import send.SendService.Events.ProductEvent;

public interface ProductService {

    public String saveProduct (ProductEvent productEvent) throws Exception;

    public ProductEvent getProductDetails(int id);

}
