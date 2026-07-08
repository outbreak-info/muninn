alter table annotations_papers add constraint uq_annotations_papers_annotation_paper_pair
unique (paper_id, annotation_id);
