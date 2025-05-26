use serde::Deserialize;
//Deserialize: Allows instances of DbConfig to be created from formats like JSON, TOML, or YAML using libraries such as serde. This means you can easily load your config from a file or environment variable.
//Debug: Allows you to print the struct using the {:?} formatter, which is useful for debugging.

#[derive(Deserialize, Debug)]
pub struct DbConfig {
    pub path: String,
}
