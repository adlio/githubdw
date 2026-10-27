//! Warehouse administration: configuration, monitored sources, groups.

use githubdw::GithubDW;
use githubdw::storage::config_repository;

use crate::cli::{ConfigCommand, GroupAction, GroupCommand, MonitorCommand};

pub fn config(warehouse: GithubDW, command: ConfigCommand) -> githubdw::Result<()> {
    let connection = warehouse.connection();
    match command {
        ConfigCommand::Get { key } => match config_repository::get(connection, &key)? {
            Some(value) => println!("{value}"),
            None => return Err(githubdw::Error::NotFound(format!("config key '{key}'"))),
        },
        ConfigCommand::Set { key, value } => {
            config_repository::set(connection, &key, &value)?;
            println!("{key} = {value}");
        }
        ConfigCommand::List => {
            for (key, value) in config_repository::list(connection)? {
                println!("{key} = {value}");
            }
        }
    }
    Ok(())
}

pub fn monitor(warehouse: GithubDW, command: MonitorCommand) -> githubdw::Result<()> {
    let connection = warehouse.connection();
    match command {
        MonitorCommand::AddRepo { repository } => {
            githubdw::storage::monitor_repository::add_repo(connection, &repository)?;
            println!("monitoring repo {}", repository.to_lowercase());
        }
        MonitorCommand::AddUser { login } => {
            githubdw::storage::monitor_repository::add_user(connection, &login)?;
            println!("monitoring user {}", login.to_lowercase());
        }
        MonitorCommand::AddOrg { org } => {
            githubdw::storage::monitor_repository::add_org(connection, &org)?;
            println!("monitoring org {}", org.to_lowercase());
        }
        MonitorCommand::List => {
            let sources = githubdw::storage::monitor_repository::list(connection)?;
            if sources.is_empty() {
                println!("nothing monitored yet");
            }
            for source in sources {
                println!(
                    "{}\t{}\t{}\tlast sync: {}",
                    source.source_type,
                    source.identifier,
                    if source.sync_enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    source.last_sync_at.unwrap_or_else(|| "never".into())
                );
            }
        }
        MonitorCommand::Remove { source } => {
            let removed = githubdw::storage::monitor_repository::remove(connection, &source)?;
            if removed == 0 {
                return Err(githubdw::Error::NotFound(format!(
                    "monitored source '{source}'"
                )));
            }
            println!("removed {source}");
        }
    }
    Ok(())
}

pub fn group(warehouse: GithubDW, command: GroupCommand) -> githubdw::Result<()> {
    let connection = warehouse.connection();
    let (kind, action) = match command {
        GroupCommand::User { action } => (githubdw::groups::GroupKind::User, action),
        GroupCommand::Repo { action } => (githubdw::groups::GroupKind::Repo, action),
    };
    match action {
        GroupAction::Create { name, description } => {
            githubdw::groups::create_group(connection, kind, &name, description.as_deref())?;
            println!("created group {name}");
        }
        GroupAction::Delete { name } => {
            githubdw::groups::delete_group(connection, kind, &name)?;
            println!("deleted group {name}");
        }
        GroupAction::Show { name } => {
            for member in githubdw::groups::members(connection, kind, &name)? {
                println!("{member}");
            }
        }
        GroupAction::List => {
            for (name, description, count) in githubdw::groups::list_groups(connection, kind)? {
                println!(
                    "{name}\t{count} members\t{}",
                    description.unwrap_or_default()
                );
            }
        }
        GroupAction::Add { name, members } => {
            githubdw::groups::add_members(connection, kind, &name, &members)?;
            println!("added {} member(s) to {name}", members.len());
        }
        GroupAction::Remove { name, members } => {
            githubdw::groups::remove_members(connection, kind, &name, &members)?;
            println!("removed {} member(s) from {name}", members.len());
        }
        GroupAction::Set { name, members } => {
            githubdw::groups::set_members(connection, kind, &name, &members)?;
            println!("set {name} to {} member(s)", members.len());
        }
    }
    Ok(())
}
