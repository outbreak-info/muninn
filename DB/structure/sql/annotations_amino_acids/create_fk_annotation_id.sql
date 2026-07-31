alter table annotations_amino_acids add constraint fk_annotations_amino_acids_annotation_id_annotations
foreign key (annotation_id) references annotations (id);
