package receive.ReceiveService.Respository;

import org.springframework.data.jpa.repository.JpaRepository;
import receive.ReceiveService.Entity.Product;

public interface ProductRepository extends JpaRepository<Product,Integer> {
}
