import javax.xml.datatype.Duration;
import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Поменять алгоритм, т.к. ключи mysql и mysqldump => --max_allowed_packet не работают.
 * Найти my.cnf
 * Запомнить текущее значение max_allowed_packet
 * Поменять значение этого параметра и ребутнуть mysql
 * потом начать импорт или экспорт
 * <p>
 * Сначала тут глянуть
 * https://yadi.sk/i/hH42TTKFg94rfQ
 */

public class Dumper {

    static final String timePattern = "dd.MM.yyyy HH:mm:ss"; // ""yyyyMMddHHmmss";
    static ArrayList<String> errorsList = new ArrayList<String>();
    static int maxAllowedPacket = 1024;

    /**
     * Использование сжатия таблиц (bzip2)
     */
    public static boolean compression = true;

    /**
     * Учетные данные для подключения к базе данных.
     * Разделены точкой с запятой чтобы далее собрать массив.
     * Было бы неплохо внести функционал автоопределения этих настроект из
     * конфигурационных файлов популярных CMS/Frameworks/CRM
     */
    public static String credentials = "localhost;website;root;bwe319tuu2";

    public static MySQLConnector mysql;

    public static BufferedReader reader;

    public static void main(String[] args) throws Exception {

        /* Получаем инструкции от пользователя. Дамп или восстановление */
        reader = new BufferedReader(new InputStreamReader(System.in));

        System.out.print("Введите учетные данные для доступа к базе MySQL.\nРазделите их точкой с запятой." +
            "\nПример: host;dbname;login;password\n>>> ");
        //credentials = reader.readLine();
        System.out.println("");
        System.out.print("Введите размер max_allowed_packet в MB\n" +
            "Этот параметр будет временно установлен в my.cnf\n>>> ");
        //maxAllowedPacket = Integer.parseInt(reader.readLine());

        // wait_timeout
        // System.out.println("Введите таймаут в секундах");

        /* Сразу после получения учетных данных устанавливаем соединение */
        mysql = new MySQLConnector(credentials);

        /*
            Пробуем подключиться к базе данных.
            При ошибке будет выброшено SQLException.
         */
        if (mysql.connected()) {

            /* Ветвление на дамп или восстановление данных */
            System.out.print("Восстановление(r) или бекап(b)?\n>>> ");
            String choise = reader.readLine();
            System.out.println("");

            /* Восстановление */
            if (choise.equals("r")) {
                System.out.println("Выбрано восстановление базы.");
                System.out.print("Укажите название папки с бекапами:\n>>> ");
                restoration(reader.readLine());
                mysql.connection.close();
            }

            /* Дамп */
            if (choise.equals("b")) {
                System.out.println("Запускаем бекап базы данных.");
                backup();
            }

            /* Вывод ошибок */
            if (errorsList.size() > 0) {
                System.out.println("Ошибки -=>");
                for (String error : errorsList) {
                    System.out.println(error);
                }
            }

        }
    }

    /**
     * Восстановление таблиц
     */
    public static void restoration(String pathFolder) throws IOException, InterruptedException {

        /* Начало времени обработки, оно же название каталога для сохранения файлов */
        ZonedDateTime currentTime = ZonedDateTime.now();
        String startTime = currentTime.format(DateTimeFormatter.ofPattern(timePattern));
        System.out.println(startTime);


        File folder = new File(pathFolder);
        File[] files = folder.listFiles();

        /* Проверить есть ли файлы */
        if (files != null) {

            System.out.println("Найдено " + files.length + " файлов.");

            int j = 1;

            for (File f : files) {
                String fileName = f.getName();

                /* Проверить формат: .sql или .sql.bz2 */
                StringBuilder command = new StringBuilder();

                // bzip2
                if (StringWorker.findByRegex(fileName, ".sql.bz2$")) {
                    command
                        .append("bunzip2 < ").append(pathFolder)
                        .append("/").append(fileName).append(" | ");
                }

                // статичная часть команды
                command
                    .append("mysql -u").append(mysql.login).append(" -p")
                    .append(mysql.password).append(" ").append(mysql.dbname);

                // просто sql
                if (StringWorker.findByRegex(fileName, ".sql$")) {
                    command
                        .append(" < ").append(pathFolder)
                        .append("/").append(fileName);
                }


                String[] cmd = {"/bin/sh", "-c", command.toString()};

                Process proc = Runtime.getRuntime().exec(cmd);

                BufferedReader errors = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

                String e = null;
                while ((e = errors.readLine()) != null) {
                    errorsList.add(e);
                }

                System.out.println("Файл " + fileName + " обработан. (" + j + ")");
                j++;
            }
        } else {
            System.out.println("Нет файлов");
        }

        System.out.println(currentTime.format(DateTimeFormatter.ofPattern(timePattern)));
        System.exit(0);
    }

    public static void backup() throws Exception {

        System.out.println("Будем использовать сжатие bzip (y/n)?\n>>> ");
        compression = reader.readLine().equals("y");

        /* Начало времени обработки, оно же название каталога для сохранения файлов */
        ZonedDateTime currentTime = ZonedDateTime.now();
        String startTime = currentTime.format(DateTimeFormatter.ofPattern(timePattern));
        String folderName = currentTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        /*
          Строка запроса для получения сведений о всех таблицах
          в базе данных с их именами и размерами.
         */
        String sql = "select " +
            "table_name, " +
            "round(((data_length + index_length) / 1024 / 1024), 2) as size " +
            "from information_schema.tables " +
            "where table_schema = '" + mysql.dbname + "' order by size;";

        /* Создаем новую директорию для сохранения файлов */
        File oFile = new File(folderName);
        if (oFile.mkdir()) {
            System.out.println("Дампы таблиц будут загружены в папку " + folderName + ".");
        }

        // запуск
        System.out.println("Запуск бекапа в " + startTime);

        /* Ключ для игнорирования таблицы при бекапе. */
        String ignoreKey = "--ignore-table=" + mysql.dbname + ".";

        /* Авторизационные данные */
        String commandStart = "mysqldump -u" + mysql.login + " -p" + mysql.password;

        try {
                /*
                    Создаем стейтмент для подготовки
                    выполнения запроса в базу данных
                    из уже существующего соединения.
                 */
            Statement statement = mysql.connection.createStatement();

                /*
                    Выполняем запрос и создаем сет результатов.
                 */
            ResultSet resultSet = statement.executeQuery(sql);

                /*
                    Если мы получили какие-то данные,
                    идем в цикле по всем строкам, в данном случае
                    у каждой строки есть две колонки:
                    (double)table_name и (String)size
                 */
            int j = 1;
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);

                /* Дампим отдельно каждую таблицу */
                dump(
                    commandStart + " "
                        + mysql.dbname + " "
                        + tableName + " | bzip2 > "
                        + folderName + "/" + tableName + ".sql.bz2"
                );

                System.out.print("Таблица " + tableName + " выгружена. (" + j + ")");
                j++;
            }

            System.out.println("\n");

            String finishTime = ZonedDateTime.now().format(DateTimeFormatter.ofPattern(timePattern));
            System.out.println("Скрипт закончил работу " + finishTime);

        } catch (SQLException | IOException | InterruptedException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Создание дампа базы данных или таблицы
     * todo переписать на работу с файлами
     *
     * @param command - основная команда для создания дампа
     */
    private static void dump(String command) throws Exception {
        /*
            Если передать в exec() строку с символом ">"
            то она не будет правильно разобрана, в некоторых случаях
            ошибка выведена НЕ будет.
         */
        String[] cmd = {"/bin/sh", "-c", command};

        /* Выполняем команду */
        Process proc = Runtime.getRuntime().exec(cmd);

        BufferedReader errors = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        String e = null;

        while ((e = errors.readLine()) != null) {
            // [Warning]
            if (!e.contains("[Warning]")) {
                errorsList.add(e);
            }

        }

        /* Ждем пока proc завершит работу */
        if (proc.waitFor() == 0) {

            /* Читаем входящий поток */
            InputStream inputStream = proc.getInputStream();

            /*
                todo в данном случае лучше получить путь до sh через which
                http://ezeon.in/blog/2014/01/execute-mysqldump-command-to-take-database-backup-from-java-code/
             */

            /* Складываем все в буфер */
            byte[] buffer = new byte[inputStream.available()];

            /* Читаем буфер */
            inputStream.read(buffer);

            String str = new String(buffer);
            System.out.println(str);
        } else {
            /* Читаем вывод возможных ошибок и делаем все тоже самое */
            InputStream errorStream = proc.getErrorStream();
            byte[] buffer = new byte[errorStream.available()];
            errorStream.read(buffer);

            String str = new String(buffer);
            System.out.println(str);
        }
    }
}
