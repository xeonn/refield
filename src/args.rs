use clap::{Arg, Command};

/// Struct to represent command-line arguments
#[derive(Debug)]
pub struct Args {
    pub db_url: String,       // URL of the CouchDB database
    pub table_name: String,   // Name of the table (or document type)
    pub old_field: String,    // Old field name to be renamed (supports dot notation for nested fields)
    pub new_field: String,    // New field name to replace the old one
    pub dry_run: bool,        // Whether to perform a dry run (preview changes without modifying the database)
    pub limit: usize,         // Maximum number of documents to fetch per iteration
}

/// Parse command-line arguments using `clap`
pub fn parse_args() -> Result<Args, String> {
    let name = env!("CARGO_PKG_NAME");
    let version = env!("CARGO_PKG_VERSION");
    let authors = env!("CARGO_PKG_AUTHORS");
    let description = env!("CARGO_PKG_DESCRIPTION");

    let matches = Command::new(name)
        .version(version)
        .author(authors)
        .about(description)
        .arg(
            Arg::new("db_url")
                .short('u')
                .long("url")
                .value_name("URL")
                .help("URL of the CouchDB database")
                .required(true),
        )
        .arg(
            Arg::new("table_name")
                .short('t')
                .long("table")
                .value_name("TABLE")
                .help("Name of the table (or document type)")
                .required(true),
        )
        .arg(
            Arg::new("old_field")
                .short('o')
                .long("old")
                .value_name("OLD_FIELD")
                .help("Old field name to be renamed (supports dot notation for nested fields)")
                .required(true),
        )
        .arg(
            Arg::new("new_field")
                .short('n')
                .long("new")
                .value_name("NEW_FIELD")
                .help("New field name to replace the old one")
                .required(true),
        )
        .arg(
            Arg::new("dry_run")
                .long("dry-run") // Use --dry-run to enable dry-run mode
                .help("Enable dry-run mode (preview changes without modifying the database)")
                .action(clap::ArgAction::SetTrue) // Defaults to false unless --dry-run is provided
                .default_value("false"), // Default value is false (not dry-run)
        )
        .arg(
            Arg::new("limit")
                .short('l')
                .long("limit")
                .value_name("LIMIT")
                .default_value("1000")
                .value_parser(clap::value_parser!(usize))
                .help("Maximum number of documents to fetch per iteration"),
        )
        .get_matches();

    // Extract arguments from matches
    let db_url = matches.get_one::<String>("db_url").unwrap().clone();
    let table_name = matches.get_one::<String>("table_name").unwrap().clone();
    let old_field = matches.get_one::<String>("old_field").unwrap().clone();
    let new_field = matches.get_one::<String>("new_field").unwrap().clone();
    let dry_run = *matches.get_one::<bool>("dry_run").unwrap_or(&false);
    let limit = *matches.get_one::<usize>("limit").unwrap_or(&1000);

    // Validate that the paths (excluding the last key) are identical
    let old_path: Vec<&str> = old_field.split('.').collect();
    let new_path: Vec<&str> = new_field.split('.').collect();

    if old_path.len() != new_path.len() {
        return Err(format!(
            "Error: The paths for 'old_field' and 'new_field' must have the same depth. \
             Found 'old_field' with {} levels and 'new_field' with {} levels.",
            old_path.len(),
            new_path.len()
        ));
    }

    if old_path[..old_path.len() - 1] != new_path[..new_path.len() - 1] {
        return Err(format!(
            "Error: The paths for 'old_field' and 'new_field' must be identical up to the last key. \
             Found 'old_field' path: {:?} and 'new_field' path: {:?}.",
            &old_path[..old_path.len() - 1],
            &new_path[..new_path.len() - 1]
        ));
    }

    Ok(Args {
        db_url,
        table_name,
        old_field,
        new_field,
        dry_run,
        limit,
    })
}

// TODO: Add unit tests for the `parse_args` function
