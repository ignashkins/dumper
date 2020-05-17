import java.sql.*;

public class MySQLConnector {

    /**
     * Урл для подключения к базе данных,
     * использую драйвер MySQL [mysql-connector.jar]
     */
    private String url;

    /**
     * Учетные данные для подклчюения к
     * базе данных
     */
    protected String dbname;
    protected String login;
    protected String password;

    /**
     * Инициализация объекта для хранения
     * соединения с базой данных
     */
    public Connection connection;

    public MySQLConnector(String userCredentials) {

        /*
            Пользователь вводит доступы к базе данных
            в одной строке в формате: host;dbname;login;password
            Приводим строку введенную пользователем к массиву
         */
        String[] credentials = userCredentials.split(";");

        try {
            /* Адрес для подключения к базе данных */
            this.url = "jdbc:mysql://" + credentials[0] + "/" + credentials[1] + "?serverTimezone=Europe/Moscow&useSSL=false";
            this.dbname = credentials[1];
            this.login = credentials[2];
            this.password = credentials[3];
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Подключение к базе данных
     *
     * @return boolean
     */
    public boolean connected() {
        try {
            /*
                DriverManager создает подключение к базе данных
                принимая в себя три параметра: адрес подключения,
                логин и пароль
             */
            // System.out.println(this.url + ";" + this.login + ";" + this.password);
            this.connection = DriverManager.getConnection(this.url, this.login, this.password);
            System.out.println("\nСоединение с базой данных установлено.\n");
            return true;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return false;
    }


}
