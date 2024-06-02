package rptu.thesis.npham.dsclient.controller;

import com.univocity.parsers.common.TextParsingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.reactive.function.client.WebClient;
import rptu.thesis.npham.dsclient.Model.Form;
import rptu.thesis.npham.dsclient.Model.QueryForm;
import rptu.thesis.npham.dsclient.service.Profiler;
import rptu.thesis.npham.dscommon.model.dto.RequestObject;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.query.QueryResults;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;
import rptu.thesis.npham.dscommon.utils.CSVReader;
import rptu.thesis.npham.dscommon.utils.Pair;
import tech.tablesaw.api.Table;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Controller
public class AppController {

    private final WebClient client;
    private final Profiler profiler;

    @Autowired
    public AppController(WebClient client, Profiler profiler) {
        this.client = client;
        this.profiler = profiler;
    }

    @GetMapping("/")
    public String start() {
        return "redirect:/query";
    }

    /**
     * Display the dataset upload form
     */
    @GetMapping("/upload_file")
    public String uploadFile(Model model, @ModelAttribute("error") Optional<String> error, @ModelAttribute("success") Optional<String> success) {
        model.addAttribute("form", new Form());
        model.addAttribute("error", error.orElse(null));
        model.addAttribute("success", success.orElse(null));
        return "upload_file";
    }

    /**
     * Retrieve the dataset using the path provided in the form and profile it, sending them
     * as a request object to the server with Rest API
     */
    @PostMapping("/upload_file")
    public String saveFile(@ModelAttribute("form") Form form, Model model) {
        model.addAttribute("form", new Form());
        if (pathNotValid(form.getPath())) {
            model.addAttribute("error", "Invalid path");
            return "upload_file";
        }
        try {
            Path path = Path.of(form.getPath());
            Table table = CSVReader.readTable(path, true);
            List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table);
            List<RequestObject> request_objects = metadata_list.stream().
                    map(pair -> new RequestObject(pair.first(), pair.second())).toList();
            client.post()
                    .uri("/save")
                    .bodyValue(request_objects)
                    .retrieve()
                    .bodyToMono(Void.class)
                    .block();
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "upload_file";
        }
        model.addAttribute("success", "Dataset uploaded successfully!");
        return "upload_file";
    }

    /**
     * Display the datasets upload form
     */
    @GetMapping("/upload_folder")
    public String uploadFolder(Model model, @ModelAttribute("error") Optional<String> error, @ModelAttribute("warning") Optional<String> warning, @ModelAttribute("success") Optional<String> success) {
        model.addAttribute("form", new Form());
        model.addAttribute("error", error.orElse(null));
        model.addAttribute("warning", warning.orElse(null));
        model.addAttribute("success", success.orElse(null));
        return "upload_folder";
    }

    /**
     * Retrieve the datasets using the path provided in the form and profile it, sending them
     * as a request object to the server with Rest API
     */
    @PostMapping("/upload_folder")
    public String saveFolder(@ModelAttribute("form") Form form, Model model) {
        model.addAttribute("form", new Form());
        String folder_path = form.getPath();
        if (pathNotValid(folder_path)) {
            model.addAttribute("error", "Invalid path");
            return "upload_folder";
        }
        List<RequestObject> all_request_objects = new ArrayList<>();
        try {
            Stream<Path> paths = Files.list(Paths.get(folder_path));
            List<Path> files = paths.toList();
            List<String> skipped = new ArrayList<>();
            for (Path file: files) {
                System.out.println("Reading " + file.toString());
                Table table;
                try {
                    table = CSVReader.readTable(file, true);
                } catch (ArrayIndexOutOfBoundsException | TextParsingException e2) {
                    skipped.add(file.getFileName().toString());
                    continue;
                }
                List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table);
                all_request_objects.addAll(metadata_list.stream().
                        map(pair -> new RequestObject(pair.first(), pair.second())).toList());
            }
            if (!skipped.isEmpty())
                model.addAttribute("warning", "Skipped: " + String.join(", ", skipped));
            paths.close();
            client.post()
                    .uri("/save")
                    .bodyValue(all_request_objects)
                    .retrieve()
                    .bodyToMono(Void.class)
                    .block();
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "upload_folder";
        }
        model.addAttribute("success", "Datasets uploaded successfully!");
        return "upload_folder";
    }

    /**
     * Display the query form
     */
    @GetMapping("/query")
    public String showQuery(Model model, @ModelAttribute("error") Optional<String> error) {
        QueryForm form = new QueryForm();
        form.setLimit(10);
        model.addAttribute("form", form);
        model.addAttribute("error", error.orElse(null));
        return "query";
    }

    /**
     * Retrieve the dataset using the path provided in the form and profile it, sending them
     * as a request object to the server with Rest API. The query results are then retrieved
     * and used to display the results in a separate page.
     */
    @PostMapping("/query")
    public String sendQuery(@ModelAttribute("form") QueryForm form, Model model) {
        if (pathNotValid(form.getPath())) {
            model.addAttribute("form", new QueryForm(form.getPath()));
            model.addAttribute("error", "Invalid path");
            return "query";
        }
        Path path = Path.of(form.getPath());
        Table table;
        try {
            table = CSVReader.readTable(path, true);
        } catch (Exception e) {
            model.addAttribute("form", new QueryForm(form.getPath()));
            model.addAttribute("error", e.getMessage());
            return "query";
        }
        List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table);
        List<RequestObject> request_objects = metadata_list.stream().
                map(pair -> new RequestObject(pair.first(), pair.second())).toList();
        QueryResults results = client.post()
                .uri(builder -> builder
                        .path("/query")
                        .queryParam("mode", form.getMode())
                        .queryParam("limit", form.getLimit())
                        .queryParam("threshold", form.getThreshold())
                        .build())
                .bodyValue(request_objects)
                .retrieve().bodyToMono(QueryResults.class).block();
        model.addAttribute("results", results);
        return "result";
    }

    /**
     * Validate that the string follows the regex of a path
     */
    private boolean pathNotValid(String path) {
        File file = new File(path);
        return !file.exists();
    }
}
