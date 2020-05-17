public class MySQLTable {

    private final String name;
    private final Boolean size;

    public MySQLTable(String name, Boolean size) {
        this.name = name;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public Boolean getSize() {
        return size;
    }
}
