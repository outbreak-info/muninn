alter table annotations_papers add constraint fk_annotations_papers_paper_id_papers
foreign key (paper_id) references papers (id);
