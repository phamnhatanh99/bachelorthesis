package rptu.thesis.npham.ds.model;

import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public class MetadataForm {
    private MultipartFile file;
    private String separator;
    private List<String> columns_types;

    public MetadataForm() {
    }

    public MetadataForm(MultipartFile file, String separator, List<String> columnsTypes) {
        this.file = file;
        this.separator = separator;
        this.columns_types = columnsTypes;
    }

    public MultipartFile getFile() {
        return file;
    }

    public void setFile(MultipartFile file) {
        this.file = file;
    }

    public List<String> getColumnsTypes() {
        return columns_types;
    }

    public void setColumnsTypes(List<String> columns_types) {
        this.columns_types = columns_types;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
