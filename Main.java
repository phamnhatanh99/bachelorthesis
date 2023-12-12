import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws SQLException {
        Predictor predictor = new Predictor();
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\nChoose a table or type 'exit' to quit: " + TPCH.listTables());
            String line = scanner.nextLine();
            if (line.equals("exit")) break;
            if (!TPCH.stringList().contains(line)) continue;
            predictor.predict(line);
        }
    }
}

