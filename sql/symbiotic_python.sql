create table if not exists symbiotic_config
(
    type  text,
    env   text,
    key   text,
    value text,
    primary key (type, env, key)
);

SELECT pg_catalog.pg_extension_config_dump('symbiotic_config', 'WHERE type = ''python venv''');

select create_venv(current_database());

create function venv_path() returns text as
$$
select venv_path(current_database());
$$ language sql;

create function create_venv() returns text as
$$
select create_venv(current_database());
$$ language sql;

create function drop_venv()
    returns text as
$$
select drop_venv(current_database());
$$ language sql;

select create_venv(current_database());
insert into symbiotic_config(type, env, key, value)
values ('python venv', current_database(), 'VIRTUAL_ENV', venv_path());

create table symbiotic.log
(
    id      serial primary key,
    content text
);