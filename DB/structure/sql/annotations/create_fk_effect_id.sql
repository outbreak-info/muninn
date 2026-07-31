alter table annotations add constraint fk_annotations_effect_id_effects
foreign key (effect_id) references effects (id);
