create table amino_acids (
	id serial not null,
	position_aa integer not null,
	ref_aa text not null,
	alt_aa text not null,
	gff_feature text not null,
	ref_codon text not null,
	alt_codon text not null
)