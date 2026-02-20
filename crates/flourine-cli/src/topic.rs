//! Topic management commands.

use anyhow::Result;
use clap::Subcommand;

use crate::client::FlourineClient;

#[derive(Subcommand)]
pub enum TopicAction {
    /// List all topics
    List,
    /// Create a new topic
    Create {
        name: String,
        #[arg(long, default_value = "1")]
        partitions: i32,
        #[arg(long)]
        retention_hours: Option<i32>,
    },
    /// Get topic details
    Get { id: i32 },
    /// Update topic settings
    Update {
        id: i32,
        #[arg(long)]
        retention_hours: i32,
    },
    /// Delete a topic
    Delete { id: i32 },
}

pub async fn run(action: TopicAction, client: &FlourineClient) -> Result<()> {
    match action {
        TopicAction::List => {
            let topics = client.list_topics().await?;
            if topics.is_empty() {
                println!("No topics found.");
                return Ok(());
            }
            println!(
                "{:<6} {:<30} {:<12} {:<16} Created",
                "ID", "Name", "Partitions", "Retention (hrs)"
            );
            println!("{}", "-".repeat(80));
            for t in topics {
                println!(
                    "{:<6} {:<30} {:<12} {:<16} {}",
                    t.topic_id,
                    t.name,
                    t.partition_count,
                    t.retention_hours,
                    t.created_at.format("%Y-%m-%d %H:%M"),
                );
            }
        }
        TopicAction::Create {
            name,
            partitions,
            retention_hours,
        } => {
            let resp = client.create_topic(&name, partitions, retention_hours).await?;
            println!("Created topic {} (id={})", name, resp.topic_id);
        }
        TopicAction::Get { id } => {
            let t = client.get_topic(id).await?;
            println!("Topic ID:     {}", t.topic_id);
            println!("Name:         {}", t.name);
            println!("Partitions:   {}", t.partition_count);
            println!("Retention:    {} hours", t.retention_hours);
            println!("Created:      {}", t.created_at.format("%Y-%m-%d %H:%M:%S"));
        }
        TopicAction::Update {
            id,
            retention_hours,
        } => {
            client.update_topic(id, retention_hours).await?;
            println!("Updated topic {id}");
        }
        TopicAction::Delete { id } => {
            client.delete_topic(id).await?;
            println!("Deleted topic {id}");
        }
    }
    Ok(())
}
