alter table papers add constraint uq_papers_authors_title_year unique (authors, publication_year, title);
