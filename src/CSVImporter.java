/* Imports the data to an SQLite database. Should only be run once.
 * 
 * First creates two tables: journey and station. Then reads the csv
 * files and inserts their data into the tables accordingly.
 * 
 * On Windows commandline: javac CSVImporter.java,
 * then: java -classpath ".;sqlite-jdbc-3.40.0.0.jar" CSVImporter 
 * 
 * Function dropTable() is not being used for anything at the moment.
 */


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class CSVImporter {
     
    static String DATABASE = "jdbc:sqlite:journeys.db";
    //static String STATIONFILE = "Helsingin_ja_Espoon_kaupunkipy%C3%B6r%C3%A4asemat_avoin.csv";

    static String STATIONFILE = "stationtest.csv"; // TEST
    
    public static void main(String[] args) {

        ArrayList<String> journeyFiles = new ArrayList<>();
        //journeyFiles.addAll(Arrays.asList("2021-05.csv", "2021-06.csv", "2021-07.csv"));

        journeyFiles.add("journeytest.csv"); // ALSO TEST
        
        if (!tableExists("station")) {
            createStationTable();
            importData(STATIONFILE,"station");
        }

        if (!tableExists("journey")) {
            createJourneyTable();
            for (String file : journeyFiles) {
                importData(file, "journey");
            }
        }
        
    }

    public static void createStationTable() {
        
        try {
           
            Connection connection = DriverManager.getConnection(DATABASE);

            String stationTable = """
                CREATE TABLE station (
                    fid 			INTEGER,
                    id 				VARCHAR(3),
                    nimi 			VARCHAR(50),
                    namn 			VARCHAR(50),
                    name 			VARCHAR(50),
                    osoite   		VARCHAR(80),
                    adress   		VARCHAR(80),
                    city 			VARCHAR(30),
                    stad 			VARCHAR(30),
                    operator 		VARCHAR(50),
                    capacity 		INTEGER,
                    x_coordinate 	REAL,
                    y_coordinate 	REAL,
                    PRIMARY KEY (id),
                    UNIQUE (name)
                )
                """;

            connection.createStatement().execute(stationTable);

            connection.close();

        } catch (Exception e) {
            System.out.println("Database problem");
            e.printStackTrace();
        }

    }

    public static void createJourneyTable() {
        try {
            
            Connection connection = DriverManager.getConnection(DATABASE);

            String journeyTable = """
                CREATE TABLE journey (
                    departure  			DATETIME,
                    return 				DATETIME,
                    dep_station_id 		VARCHAR(3),
                    dep_station_name 	VARCHAR(50),
                    ret_station_id 		VARCHAR(3),
                    ret_station_name 	VARCHAR(50),
                    distance 			INTEGER,
                    duration 			INTEGER,
                    FOREIGN KEY (dep_station_id) REFERENCES station(id),
                    FOREIGN KEY (ret_station_id) REFERENCES station(id)
                )
                """;

            connection.createStatement().execute(journeyTable);

            connection.close();

        } catch (Exception e) {
            System.out.println("Database problem");
            e.printStackTrace();
        }

    }

    public static void importData(String fileName, String tableName) {

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
           
            Connection connection = DriverManager.getConnection(DATABASE);

            String line = "";
            boolean ignoreLine = true;
            while ((line = br.readLine()) != null) {

                // Ignores the header line
                if (ignoreLine) {
                    ignoreLine = false;
                    continue;
                }

                String[] values = line.split(",");

                for (String value : values) {
                    if (value.isEmpty()) {
                        value = "NULL";
                    }
                }

                if (tableName.equals("journey")) {
                    insertJourneyRow(values, connection);
                } else if (tableName.equals("station")) {
                    insertStationRow(values, connection);
                }
                
            }
            connection.close();

        } catch (Exception e) {
            System.out.println("Error reading file 2021-05");
            e.printStackTrace();
        }
    }   

    private static void insertJourneyRow (String[] values, Connection conn) {

        try {

            int distance = Integer.parseInt(values[6]);
            int duration = Integer.parseInt(values[7]);

            if (distance < 10 || duration < 10 || values.length < 8) {
                return;
            }

            PreparedStatement statement = conn.prepareStatement(
            "INSERT INTO journey VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

            statement.setString(1, values[0].replace("T", " "));
            statement.setString(2, values[1].replace("T", " "));
            for (int i = 2; i < 6; i++) {
                statement.setString(i + 1, values[i]);
            }
            statement.setInt(7, distance);
            statement.setInt(8, duration);

            statement.execute();

        } catch (Exception e) {
            System.out.println("Inserting journey failed.");
            e.printStackTrace();
        }
        
    }

    private static void insertStationRow (String[] values, Connection conn) {

        try {

            if (values.length < 13) {
                return;
            }

            PreparedStatement statement = conn.prepareStatement(
            "INSERT INTO station VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            statement.setInt(1, Integer.parseInt(values[0]));
            statement.setString(2, values[1]);
            for (int i = 2; i < 10; i++) {
                statement.setString(i + 1, values[i]);
            }
            statement.setInt(11, Integer.parseInt(values[10]));
            statement.setDouble(12, Double.parseDouble(values[11]));
            statement.setDouble(13, Double.parseDouble(values[12]));

            statement.execute();

        } catch (Exception e) {
            System.out.println("Inserting station failed.");
            e.printStackTrace();
        }
    }

    private static void dropTable (String tableName) {
        try {
        
            Connection connection = DriverManager.getConnection(DATABASE);
            connection.createStatement().execute("DROP TABLE " + tableName);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        

    }

    private static boolean tableExists (String tableName) {
        try {

            Connection connection = DriverManager.getConnection(DATABASE);
            String query = "PRAGMA table_info(" + tableName + ")"; 
            ResultSet results = connection.createStatement().executeQuery(query);

            if (results.next()) {
                results.close();
                connection.close();
                return true;
            } else {
                connection.close();
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}