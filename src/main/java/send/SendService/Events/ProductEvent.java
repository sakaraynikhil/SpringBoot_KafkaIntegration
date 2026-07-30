package send.SendService.Events;

public class ProductEvent {


    private int id;

    private String product_name;

    private int quantity;

    public ProductEvent(){

    }

    @Override
    public String toString() {
        return "ProductEvent{" +
                "id=" + id +
                ", product_name='" + product_name + '\'' +
                ", quantity=" + quantity +
                '}';
    }

    public ProductEvent(int id, String product_name, int quantity) {
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
