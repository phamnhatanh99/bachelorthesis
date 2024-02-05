import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws SQLException {
        Predictor predictor = new Predictor("jdbc:postgresql://localhost:5432/tpch", "postgres", "1234");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\nChoose a table or type 'exit' to quit: " + predictor.listTablesAsString());
            String line = scanner.nextLine();
            if (line.equals("exit")) break;
            if (!predictor.listTables().contains(line)) continue;
            predictor.predict(line);
        }
        scanner.close();
    }
}


