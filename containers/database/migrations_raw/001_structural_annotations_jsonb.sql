-- Convert structural_note to jsonb and extract indexed columns
ALTER TABLE structural_annotations ADD COLUMN protein_name text;
ALTER TABLE structural_annotations ADD COLUMN reference_site text;
ALTER TABLE structural_annotations ADD COLUMN reference_H1_site text;
ALTER TABLE structural_annotations ADD COLUMN mature_H5_site text;
ALTER TABLE structural_annotations ADD COLUMN HA1_HA2_H5_site text;

UPDATE structural_annotations SET protein_name = structural_note->>'region' WHERE structural_note IS NOT NULL;
UPDATE structural_annotations SET reference_site = structural_note->>'reference_site' WHERE structural_note IS NOT NULL;
UPDATE structural_annotations SET reference_H1_site = structural_note->>'reference_H1_site' WHERE structural_note IS NOT NULL;
UPDATE structural_annotations SET mature_H5_site = structural_note->>'mature_H5_site' WHERE structural_note IS NOT NULL;
UPDATE structural_annotations SET HA1_HA2_H5_site = structural_note->>'HA1_HA2_H5_site' WHERE structural_note IS NOT NULL;

CREATE INDEX ix_structural_annotations_protein_name ON structural_annotations(protein_name);
CREATE INDEX ix_structural_annotations_reference_site ON structural_annotations(reference_site);
CREATE INDEX ix_structural_annotations_reference_H1_site ON structural_annotations(reference_H1_site);
CREATE INDEX ix_structural_annotations_mature_H5_site ON structural_annotations(mature_H5_site);
CREATE INDEX ix_structural_annotations_HA1_HA2_H5_site ON structural_annotations(HA1_HA2_H5_site);
