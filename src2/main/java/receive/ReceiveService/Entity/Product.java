package receive.ReceiveService.Entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "product")
public class Product {


    @Id
    @Column(name = "id")
    private int id;

    @Column(name = "product_name")
    private String product_name;

    @Column(name = "quantity")
    private int quantity;

    public Product(){

    }

    @Override
    public String toString() {
        return "ProductEvent{" +
                "id=" + id +
                ", product_name='" + product_name + '\'' +
                ", quantity=" + quantity +
                '}';
    }

    public Product(int id, String product_name, int quantity) {
        this.id = id;
        this.product_name = product_name;
        this.quantity = quantity;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getProduct_name() {
        return product_name;
    }

    public void setProduct_name(String product_name) {
        this.product_name = product_name;
    }

}
