use crate::{PgAppResult, validation::validate_queue_name};
use sqlx::postgres::PgListener;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqNotification {
    pub channel: String,
    pub payload: String,
}

pub struct MqListener {
    listener: PgListener,
    channel: String,
}

impl MqListener {
    pub async fn connect(database_url: &str, queue_name: &str) -> PgAppResult<Self> {
        let channel = mq_channel(queue_name)?;
        let mut listener = PgListener::connect(database_url).await?;
        listener.listen(&channel).await?;
        Ok(Self { listener, channel })
    }

    pub async fn recv(&mut self) -> PgAppResult<MqNotification> {
        loop {
            let notification = self.listener.recv().await?;
            if notification.channel() == self.channel {
                return Ok(MqNotification {
                    channel: notification.channel().to_string(),
                    payload: notification.payload().to_string(),
                });
            }
        }
    }
}

pub fn mq_channel(queue_name: &str) -> PgAppResult<String> {
    validate_queue_name(queue_name)?;
    Ok(format!("pgapp_mq_{queue_name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_prefixed_mq_channel_name() {
        assert_eq!(mq_channel("orders").unwrap(), "pgapp_mq_orders");
        assert!(mq_channel("bad queue").is_err());
    }
}
