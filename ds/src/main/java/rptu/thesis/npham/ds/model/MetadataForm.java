package rptu.thesis.npham.ds.model;

import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public class MetadataForm {
    private MultipartFile file;
    private List<String> columnsTypes;

    public MetadataForm() {
    }

    public MetadataForm(MultipartFile file, List<String> columnsTypes) {
        this.file = file;
        this.columnsTypes = columnsTypes;
    }

    public MultipartFile getFile() {
        return file;
    }

    public void setFile(MultipartFile file) {
        this.file = file;
    }

    public List<String> getColumnsTypes() {
        return columnsTypes;
    }

    public void setColumnsTypes(List<String> columnsTypes) {
        this.columnsTypes = columnsTypes;
    }
}
