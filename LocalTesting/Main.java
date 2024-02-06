import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Select a database to predict the join partners (tpch, imdb). Type anything else to quit.");
        String line = scanner.nextLine();
        String url = "jdbc:postgresql://localhost:5432/";
        String database;
        switch (line) {
            case "tpch":
                database = "tpch";
                break;
            case "imdb":
                database = "imdb";
                break;
            default:
                return;
        }
        Predictor predictor = new Predictor(url + database, "postgres", "1234");
        while (true) {
            System.out.println("\nChoose a table or type 'exit' to quit: " + predictor.listTablesAsString());
            line = scanner.nextLine();
            if (line.equals("exit")) break;
            if (!predictor.listTables().contains(line)) continue;
            predictor.predict(line);
        }
        scanner.close();
    }
}


