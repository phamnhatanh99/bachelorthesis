package rptu.thesis.npham.ds.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.multipart.MultipartFile;
import rptu.thesis.npham.ds.model.MetadataForm;
import rptu.thesis.npham.ds.repository.MetadataRepository;
import rptu.thesis.npham.ds.service.CSV;
import rptu.thesis.npham.ds.service.Profiler;
import tech.tablesaw.api.Table;

import java.util.List;

/**
 * Controller for the registration form.
 */
@Controller
public class RegisterController {

    private final MetadataRepository metadata_repository;

    @Autowired
    public RegisterController(MetadataRepository metadata_repository) {
        this.metadata_repository = metadata_repository;
    }

    /**
     * Displays the registration form.
     */
    @GetMapping("/register")
    public String registerForm(Model model) {
        MetadataForm metadata_form = new MetadataForm();
        model.addAttribute("metadata_form", metadata_form);
        return "register";
    }

    /**
     * Extracts metadata from the form and saves it to the database.
     */
    @PostMapping("/register")
    public String register(@ModelAttribute("metadata_form") MetadataForm metadata_form) {
        String message = validateForm(metadata_form);
        if (!message.isEmpty()) {
            System.out.println(message);
            return "redirect:/register";
        }
        MultipartFile file = metadata_form.getFile();
        List<String> column_types = metadata_form.getColumnsTypes();
        Table table = CSV.getTable(file, column_types);
        System.out.println(column_types);
        assert table != null;
        metadata_repository.insert(Profiler.profile(table));
        System.out.println("Form registered successfully.");
        return "redirect:/register";
    }

    /**
     * Validates so that the form is not empty and the file is a CSV file.
     */
    private String validateForm(MetadataForm metadata_form) {
        String message = "";
        MultipartFile file = metadata_form.getFile();
        List<String> columns_types = metadata_form.getColumnsTypes();
        String original_filename = file.getOriginalFilename();
        Table table = CSV.getTable(file, columns_types);
        if (file.isEmpty() || original_filename == null || isNotCSV(original_filename) || table == null)
            message = "Please upload a CSV file.";
        else if (numberOfColumnsNotMatch(table, columns_types))
            message = "Number of columns in the CSV file does not match the number of columns provided.";
        return message;
    }

    private boolean isNotCSV(String filename) {
        return !filename.endsWith(".csv");
    }

    private boolean numberOfColumnsNotMatch(Table table, List<String> columns_types) {
        return CSV.getColumnNames(table).size() != columns_types.size();
    }
}
