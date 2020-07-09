import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class MemoryAnalyzer {
    final static ArrayList<String[]> memoryInfoTable = new ArrayList<>();
    final static int  GIGABYTE = 1073741824;

    public static void main(String[] args)  {
        String[] pathInfo = new String[5];
        long totalStorage = 0;
        String path = System.getProperty("user.dir"); //определяем, в какой директории находится исполняемый файл

        File folder = new File(path);
        if (!folder.exists()) {
            System.out.println("Путь не найден!");
            return;
        }
        //вычисляем общий объем памяти на диске
        File[] roots = File.listRoots();
        for (File root : roots) {
            totalStorage += root.getTotalSpace();
        }

        //получаем информацию обо всех файлах/директориях и также об основной (введенной пользователем) директории
        getPathInfo(path, totalStorage);
        pathInfo[0] = path;
        pathInfo[1] = Double.toString(getFolderSize(folder) * 1.0 / GIGABYTE);
        pathInfo[2] = Double.toString(getFolderSize(folder) * 1.0  / totalStorage);
        pathInfo[3] = Double.toString(totalStorage * 1.0 / GIGABYTE);
        memoryInfoTable.add(pathInfo);

        //создаем таблицу бд
        try {
            createDbTable();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Получаем общий размер директории
    private static long getFolderSize(File folder) {
        long length = 0;
        File[]files = folder.listFiles();
        int count = files.length;

        for (int i = 0; i < count; i++) {
            if (files[i].isFile()) {
                length += files[i].length();
            }
            else {
                length += getFolderSize(files[i]);
            }
        }
        return length;
    }

    //добавляем информацию о файле/директории в список memoryInfoTable
    private static void getPathInfo(String dir, double totalStorage) {

        long fileSize;
        long folderSize;
        File folder = new File(dir);
        String[] files = folder.list();

        for (int i = 0; i < files.length; i++) {
            String[] pathInfo = new String[4];

            File f1 = new File(dir + File.separator + files[i]);

            if (f1.isFile()) {
                pathInfo[0] = f1.getAbsolutePath();
                fileSize = f1.length();
                pathInfo[1] = Double.toString(fileSize*1.0/GIGABYTE);
                pathInfo[2] = Double.toString(fileSize *1.0/ totalStorage);
                pathInfo[3] = Double.toString(totalStorage * 1.0 / GIGABYTE);
                memoryInfoTable.add(pathInfo);

            }
            else {
                folderSize = getFolderSize(f1);
                getPathInfo(dir + File.separator + files[i],totalStorage);
                pathInfo[0] = f1.getAbsolutePath();
                pathInfo[1] = Double.toString(folderSize * 1.0 / GIGABYTE);
                pathInfo[2] = Double.toString(folderSize *1.0/ totalStorage);
                pathInfo[3] = Double.toString(totalStorage * 1.0 / GIGABYTE);
                memoryInfoTable.add(pathInfo);

            }
        }
    }

    //Устанавливаем соединение с БД
    private static Connection getDBConnection() {

        String db_driver = null;
        String db_connection = null;
        String login = null;
        String password = null;

        InputStream is;
        Properties prop = new Properties();


        try {
            is = MemoryAnalyzer.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(is);

            db_driver = prop.getProperty("db_driver");
            db_connection = prop.getProperty("db_connection");
            login = prop.getProperty("login");
            password = prop.getProperty("password");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        Connection dbConnection = null;
        try {
            Class.forName(db_driver);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(db_connection, login,password);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }

    //Создаем таблицу(если она еще не создана) и добавляем строки из списка memoryInfoTable
    private static Statement createDbTable() throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        String createTableSQL = "CREATE TABLE IF NOT EXISTS MemoryInfo"
                + "(ID SERIAL NOT NULL, "
                + "PATH VARCHAR(200) NOT NULL, "
                + "SIZE REAL NOT NULL, "
                + "PERCENTOFSTORAGE REAL NOT NULL, "
                + "TOTALSTORAGE REAL NOT NULL, " + "PRIMARY KEY (ID))";

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();

            // выполнить SQL запрос
            statement.execute(createTableSQL);

            for (String[] x : memoryInfoTable) {
                String insertTableSQL = "INSERT INTO MemoryInfo" + "(ID, PATH, SIZE, PERCENTOFSTORAGE, TOTALSTORAGE)" +
                        "VALUES(DEFAULT , ? , ? , ? , ?)";
                PreparedStatement pst = dbConnection.prepareStatement(insertTableSQL);
                //pst.setInt(1,Integer.parseInt(x[0]) );
                pst.setString(1,x[0] );
                pst.setDouble(2,Double.parseDouble(x[1]) );
                pst.setDouble(3,Double.parseDouble(x[2]) );
                pst.setDouble(4,Double.parseDouble(x[3]) );
                pst.executeUpdate();

            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }
        return statement;
    }
}
