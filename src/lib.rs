use pgrx::prelude::*;
use std::env;
use std::fmt::format;
use std::process::{Command, Output};
use std::str;
use std::result::Result;
use std::fs::File;
use std::io::Write;

pgrx::pg_module_magic!();

#[pg_extern]
#[search_path(@ extschema @)]
fn create_venv(name: &str) -> Result<String, String> {
    let virtual_env = venv_path(name);
    let command = format!("python -m venv {}", virtual_env.as_str());
    run_command(command.as_str())
        .and_then(|out| -> Result<String, String> {
            pip_install(virtual_env.as_str(), "asyncpg")
                .and_then(|_output| {
                    pip_install(virtual_env.as_str(), "asyncpg-listen")
                })
                .map(|_x| -> String { out })
                .map_err(|err| -> String { err.to_string() })
        })
        .and_then(|output| -> Result<String, String> {
            let result = deploy_server(name);
            match result {
                Ok(_) => Ok(output),
                Err(err) => {
                    Err(err.to_string())
                }
            }
        })
}

#[pg_extern]
#[search_path(@ extschema @)]
fn drop_venv(name: &str) -> String {
    let home = venv_path(name);
    let command = format!("rm -rf {}", home);

    let result = run_command(command.as_str());

    match result {
        Ok(out) => out.to_string(),
        Err(err) => err,
    }
}

#[pg_extern]
#[search_path(@ extschema @)]
fn venv_path(name: &str) -> String {
    let home = get_home();

    format!("{}/.symbiotic/{}", home, name)
}

#[pg_extern]
#[search_path(@ extschema @)]
fn run_symbiotic(venv: &str, channel: &str) -> Result<String, String> {
    let home = get_home();
    let virtual_env = format!("{}/.symbiotic/{}", home, venv);

    let script = format!("{}/bin/run.sh", virtual_env);

    let child = Command::new("sh")
        .arg(script.as_str())
        .current_dir(virtual_env.as_str())
        .spawn()
        .unwrap();

    insert_config(venv, "PID", child.id().to_string().as_str()).unwrap();
    insert_config(venv, "listen_key", channel)
}

fn insert_config(name: &str, key: &str, value: &str) -> Result<String, String> {
    let query =
        format!(r"insert into symbiotic.symbiotic_config(type, env, key, value)
        values('python venv', $1, $2, $3)
        on conflict(type, env, key) do update set value=$3");

    let result = Spi::run_with_args(query.as_str(),
                                    Some(vec![(PgBuiltInOids::TEXTOID.oid(), name.into_datum()),
                                              (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                                              (PgBuiltInOids::TEXTOID.oid(), value.into_datum())]));
    match result {
        Ok(_) => Ok(value.to_string()),
        Err(err) => Err(err.to_string())
    }
}

fn pip_install(virtual_env: &str, pip: &str) -> std::io::Result<Output> {
    Command::new(format!("{}/bin/pip", virtual_env))
        .arg("install")
        .arg(pip)
        .env("VIRTUAL_ENV", virtual_env)
        .output()
}

fn run_command(command: &str) -> Result<String, String> {
    let output = if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(["/C", command])
            .output()
    } else {
        Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
    };

    output
        .map(|out| String::from_utf8(out.stdout).unwrap())
        .map_err(|err| err.to_string())
}

fn get_home() -> String {
    env::var("HOME").unwrap()
}

#[pg_extern]
#[search_path(@ extschema @)]
fn deploy_server(venv: &str) -> std::io::Result<()> {
    let home = get_home();
    let virtual_env = format!("{}/.symbiotic/{}", home, venv);

    let bytes = include_bytes!("../scripts/symbiotic.py");
    let symbiotic = format!("{}/bin/symbiotic.py", virtual_env);
    let mut file = File::create(symbiotic.as_str()).unwrap();
    file.write_all(bytes).unwrap();

    let port: String = Spi::get_one("show port").unwrap().unwrap();
    let database_name: String = Spi::get_one("select current_database()::text").unwrap().unwrap();

    let path = format!("{}/bin", virtual_env);
    let python = format!("{}/bin/python", virtual_env);
    let script = format!("{}/bin/run.sh", virtual_env);

    let source = format!(r"#!/usr/bin/env sh
export VIRTUALENV={}
export PATH={}:$PATH

nohup {} {} {} {} {} &
", path, virtual_env,
                         python, symbiotic, port, database_name, venv);

    let mut file = File::create(script.as_str()).unwrap();
    file.write_all(source.into_bytes().as_ref())
}


extension_sql_file!("../sql/symbiotic_python.sql",
    requires = [create_venv, drop_venv, venv_path]);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::fs;
    use pgrx::prelude::*;
    use std::path::Path;
    use crate::{create_venv, get_home};

    #[pg_test]
    fn test_home() {
        assert_eq!("/Users/mars", get_home());
    }

    #[pg_test]
    fn test_venv() {
        let home = get_home();
        let result = create_venv("workshop");
        println!("{}", result.as_str());
        let venv_path_string = format!("{}/.symbiotic/workshop", home);
        let venv_path = Path::new(venv_path_string.as_str());

        assert!(venv_path.exists());
        let result = fs::remove_dir_all(venv_path);
        assert!(result.is_ok());
        assert!(!venv_path.exists())
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
