alter table annotations_papers add constraint fk_annotations_papers_annotation_id_annotations
foreign key (annotation_id) references annotations (id);
