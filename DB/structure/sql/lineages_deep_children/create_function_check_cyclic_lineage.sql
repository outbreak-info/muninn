create or replace function check_cyclic_lineage()
        returns trigger as
    $$
    declare
        num_rows integer;
    begin
        select count(*)
        into num_rows
        from lineages_deep_children
        where
            child_id = new.parent_id
            and parent_id = new.child_id;
        if num_rows > 0 then
            raise exception 'cyclic lineage hierarchy';
        end if;
        return new;
    end;
    $$
        language plpgsql;