create table annotations_papers (
	id bigserial not null,
	paper_id bigint not null,
	annotation_id bigint not null,
	quotation text
);
