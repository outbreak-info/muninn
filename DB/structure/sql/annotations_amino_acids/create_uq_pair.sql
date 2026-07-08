alter table annotations_amino_acids add constraint uq_annotations_amino_acids_pair
unique (amino_acid_id, annotation_id);
