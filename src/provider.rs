use std::{collections::HashMap, hash::Hash, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use futures_util::{
    StreamExt,
    sink::SinkExt,
    stream::{SplitSink, SplitStream},
};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{http::Request, protocol::Message},
};

use crate::connection_info::ConnectionInfo;
use crate::types::*;

type SplitedStream = SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>;
type SplitedSink = SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>;

type AsyncError = dyn std::error::Error + Send + Sync;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct SeqType {
    cmd: i8,
    opcode: i64,
}

#[derive(Clone, Deserialize)]
pub struct SequenceHandler {
    values: HashMap<SeqType, i64>,
}

impl SequenceHandler {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn increase_seq(&mut self, opcode: i64, cmd: i8) {
        let seq_type = SeqType {
            cmd: cmd,
            opcode: opcode,
        };
        let clone = seq_type.clone();
        let value_to_insert = match self.values.get(&clone) {
            Some(val) => val + 1,
            None => {
                self.values.insert(clone, 0);
                0
            }
        };
        self.values.insert(seq_type, value_to_insert);
    }

    pub fn get_seq(&mut self, opcode: i64, cmd: i8) -> i64 {
        let seq_type = SeqType {
            cmd: cmd,
            opcode: opcode,
        };
        let value = match self.values.get(&seq_type) {
            Some(val) => *val,
            None => 0,
        };
        value
    }

    pub fn reset(&mut self) {
        self.values = HashMap::new();
    }
}

/// Struct used to perform requests to max's api with payload
#[derive(Serialize, Deserialize, Clone)]
pub struct RequestState {
    cmd: i8,
    opcode: i64,
    seq: i64,
    ver: i8,
    #[serde(skip_serializing)]
    sequence_handler: SequenceHandler,
    pub payload: Option<Data>,
}

impl RequestState {
    pub fn new() -> Self {
        Self {
            cmd: 0,
            ver: 11,
            opcode: 0,
            seq: 0,
            sequence_handler: SequenceHandler::new(),
            payload: None,
        }
    }

    pub fn set_opcode(&mut self, opcode: i64) {
        self.opcode = opcode;
    }

    pub fn increase_seq(&mut self) {
        self.sequence_handler.increase_seq(self.opcode, self.cmd);
        self.seq = self.sequence_handler.get_seq(self.opcode, self.cmd);
    }

    pub fn sync_seq(&mut self) {
        self.seq = self.sequence_handler.get_seq(self.opcode, self.cmd);
    }

    pub fn set_cmd(&mut self, cmd: i8) {
        self.cmd = cmd;
    }
}

pub async fn connect_to_servers(
    headers: String,
    uri: String,
) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Box<AsyncError>> {
    let mut request = Request::builder();
    let map: HashMap<&str, &str> = serde_json::from_str(&headers)?;
    request = request.uri(uri);
    for (k, v) in map {
        request = request.header(k, v);
    }

    let (stream, _) = connect_async(request.body(())?).await?;

    Ok(stream)
}

async fn check_payload_field<F>(
    field: Option<F>,
    field_name: String,
) -> Result<F, Box<AsyncError>> {
    match field {
        Some(f) => Ok(f),
        None => {
            #[cfg(debug_assertions)]
            println!("One of payload fields is empty");
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Field '{}' is required", field_name),
            )))
        }
    }
}

/// Provider is the main struct to work with max's reverse engineered api
/// Here is an example of usage:
/// '''
///let user_agent_data = Data {
///    device_id: Some(Uuid::new_v4().to_string()),
///    user_agent: Some(config.max_agent),
///    ..Default::default()
///};
///
///let auth_data = Data {
///    chats_count: Some(40),
///    chats_sync: Some(0),
///    contacts_sync: Some(0),
///    drafts_sync: Some(0),
///    interactive: Some(true),
///    presence_sync: Some(-1),
///    token: Some(token.to_string()),
///    ..Default::default()
///};
///
///let mut provider =
///    MaxProvider::new(
///        serde_json::to_string(&config.headers)?,
///        "wss://ws-api.oneme.ru/websocket".to_string(),
///        tx,
///        user_agent_data,
///        auth_data,
///    )
///    .await?
///    .attach_handler(|response| {
///        println!("{}", response.payload.message.unwrap().text);
///    });
/// '''
pub struct Provider {
    read: Arc<Mutex<SplitedStream>>,
    write: Arc<Mutex<SplitedSink>>,
    headers: String,
    uri: String,
    state: RequestState,
    full_data: Option<ResponseState>,
    user_agent: Data,
    auth_data: Data,
    connection_info: ConnectionInfo,
    named_identifiers: HashMap<i64, String>,
    handler: Option<
        Box<
            dyn Fn(ResponseState) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync
                + 'static,
        >,
    >,
}

impl Provider {
    pub async fn new(
        headers: String,
        uri: String,
        user_agent: Data,
        auth_data: Data,
    ) -> Result<Self, Box<AsyncError>> {
        let stream = connect_to_servers(headers.clone(), uri.clone()).await?;

        let (write, read) = stream.split();

        let state = RequestState::new();

        Ok(Self {
            read: Arc::new(Mutex::new(read)),
            write: Arc::new(Mutex::new(write)),
            state,
            full_data: None,
            headers,
            uri,
            user_agent,
            auth_data,
            connection_info: ConnectionInfo::new(),
            named_identifiers: HashMap::new(),
            handler: None,
        })
    }

    pub fn attach_handler<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(ResponseState) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.handler = Some(Box::new(move |state| Box::pin(f(state))));

        self
    }

    pub async fn auth(&mut self) -> Result<(), Box<AsyncError>> {
        self.state.sequence_handler.reset();
        self.state.set_opcode(6);
        let mut user_agent_state = self.state.clone();
        user_agent_state.payload = Some(self.user_agent.clone());

        self.state.set_opcode(19);
        let mut auth_data_state = self.state.clone();
        auth_data_state.payload = Some(self.auth_data.clone());

        self.write_to_stream(serde_json::to_string(&user_agent_state)?)
            .await?;
        self.write_to_stream(serde_json::to_string(&auth_data_state)?)
            .await?;

        self.connection_info.reset_retries()?;

        Ok(())
    }

    pub async fn send_data(
        &mut self,
        data: Data,
        opcode: i64,
        cmd: i8,
    ) -> Result<(), Box<AsyncError>> {
        self.state.set_opcode(opcode);
        self.state.set_cmd(cmd);
        self.state.sync_seq();
        let mut state_copy = self.state.clone();
        state_copy.payload = Some(data);
        let raw_data = serde_json::to_string(&state_copy)?;
        match self.write_to_stream(raw_data).await {
            Err(_) => {
                self.init_new_session().await?;
            }
            _ => {}
        };

        self.state.increase_seq();

        Ok(())
    }

    async fn write_to_stream(&mut self, message: String) -> Result<(), Box<AsyncError>> {
        let write = self.write.clone();
        write.lock().await.send(Message::Text(message)).await?;
        Ok(())
    }

    async fn init_new_session(&mut self) -> Result<(), Box<AsyncError>> {
        match self.connection_info.increase_retries() {
            Ok(_) => {}
            Err(_) => {
                // std::process::exit(-1);
            }
        }

        let _ = self.write.lock().await.close().await;

        let stream = connect_to_servers(self.headers.clone(), self.uri.clone()).await?;

        let (write, read) = stream.split();

        self.write = Arc::new(Mutex::new(write));
        self.read = Arc::new(Mutex::new(read));

        self.auth().await?;

        Ok(())
    }

    pub async fn handle_everything(self) -> Result<(), Box<AsyncError>> {
        let mut interaction_interval = tokio::time::interval(Duration::from_secs(30));

        let shared_self = Arc::new(Mutex::new(self));
        let shared_for_task = shared_self.clone();

        tokio::spawn(async move {
            loop {
                {
                    let mut locked_self = shared_for_task.lock().await;
                    if let Err(e) = locked_self.handle_messages().await {
                        println!("Handle messages error: {}", e);
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        loop {
            interaction_interval.tick().await;

            {
                let mut locked_self = shared_self.lock().await;
                locked_self.accept_interactions().await?;
            }
        }
    }

    async fn handle_messages(&mut self) -> Result<(), Box<AsyncError>> {
        let read_clone = Arc::clone(&self.read);
        let text = match {
            let mut guard = read_clone.lock().await;
            guard.next().await
        } {
            Some(Ok(Message::Text(text))) => text,
            Some(Err(_)) => {
                self.init_new_session().await?;
                return Ok(());
            }
            Some(_) => {
                return Ok(());
            }
            None => {
                return Ok(());
            }
        };
        let headers: ResponseHeaders = match serde_json::from_str(&text) {
            Ok(h) => h,
            Err(e) => {
                #[cfg(debug_assertions)]
                println!("Couldn't parse response headers: {:#?}", e);
                return Ok(());
            }
        };
        if cfg!(debug_assertions) {
            println!("[HEADERS]\n{}\n", headers);
        }
        let mut response: Option<ResponseState> = None;
        let opcode = headers.opcode;
        if opcode == 19 || opcode == 49 || opcode == 128 {
            response = match serde_json::from_str(&text) {
                Ok(rs) => rs,
                Err(e) => {
                    #[cfg(debug_assertions)]
                    println!(
                        "Couldn't parse response state: {:#?}. Maybe payload is empty",
                        e
                    );
                    return Ok(());
                }
            };
        }
        if opcode == 19 && response.clone().is_some() {
            self.handle_data_response(response.clone().unwrap()).await?;
        }
        if opcode == 49 && response.clone().is_some() {
            self.handle_returned_messages(response.clone().unwrap()).await?;
        }
        if opcode == 128 && response.clone().is_some() {
            self.handle_incoming_message(response.unwrap()).await?;
        }

        Ok(())
    }

    async fn handle_data_response(
        &mut self, 
        response: ResponseState
    ) -> Result<(), Box<AsyncError>> {
        self.full_data = Some(response.clone());
        let chats = check_payload_field(
            response.clone().payload.chats.clone(),
            "chats".to_string(),
        )
        .await?;
        for chat in chats.iter() {
            if let Some(title) = &chat.title {
                self.named_identifiers
                    .insert(chat.id.clone(), title.to_string());
            }
        }
        let contacts = check_payload_field(
            response.clone().payload.contacts.clone(),
            "contacts".to_string(),
        )
        .await?;
        for contact in contacts.iter() {
            self.named_identifiers
                .insert(contact.id.clone(), contact.names[0].name.clone());
        }

        Ok(())
    }

    async fn handle_returned_messages(
        &mut self,
        response: ResponseState,
    ) -> Result<(), Box<AsyncError>> {
        if let Some(maybe_empty) = &response.clone().payload.messages {
            match maybe_empty {
                MaybeEmpty::Full(msgs) => {
                    for message in msgs.clone().iter_mut() {
                        let id = match message.sender {
                            Some(id) => Some(id),
                            None => response.clone().payload.chat_id,
                        };

                        let name = match id {
                            Some(id) => self.get_name_by_id(id),
                            None => {
                                #[cfg(debug_assertions)]
                                println!("Couldn't retrieve sender id from message.");
                                return Ok(());
                            }
                        };

                        message.sender_name = Some(name.clone());

                        match &self.handler {
                            Some(f) => f(response.clone()).await,
                            None => {}
                        };
                    }
                }
                MaybeEmpty::Empty {} => {}
            }
        }
        Ok(())
    }

    async fn handle_incoming_message(
        &mut self,
        mut response: ResponseState,
    ) -> Result<(), Box<AsyncError>> {
        let mut message = match response.clone().payload.message.clone() {
            Some(msg) => msg,
            None => {
                #[cfg(debug_assertions)]
                println!("Header is 128 and there is data, but message is None");
                return Ok(());
            }
        };

        let id = match message.sender {
            Some(id) => Some(id),
            None => response.clone().payload.chat_id,
        };

        let name = match id {
            Some(id) => self.get_name_by_id(id),
            None => {
                #[cfg(debug_assertions)]
                println!("Couldn't retrieve sender id from message.");
                return Ok(());
            }
        };

        message.sender_name = Some(name.clone());

        response.payload.message = Some(message.clone());

        match &self.handler {
            Some(f) => f(response.clone().clone()).await,
            None => {}
        };

        // Answer to max that we have received the message
        self.send_data(
            Data {
                chat_id: response.clone().payload.chat_id,
                message_id: Some(message.clone().id),
                ..Default::default()
            },
            128,
            1,
        )
        .await?;

        let mut msg_ids = Vec::new();
        msg_ids.push(message.id);

        self.send_data(
            Data {
                chat_id: response.payload.chat_id,
                message_ids: Some(msg_ids),
                ..Default::default()
            },
            74,
            0,
        )
        .await?;

        Ok(())
    }

    fn get_name_by_id(&self, id: i64) -> String {
        let result = match self.named_identifiers.get(&id) {
            Some(n) => n.to_string(),
            None => "Unknown".to_string(),
        };

        result
    }

    async fn accept_interactions(&mut self) -> Result<(), Box<AsyncError>> {
        self.send_data(
            Data {
                interactive: Some(true),
                ..Default::default()
            },
            1,
            0,
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use dotenvy::dotenv;
    use min_rs_config::*;
    use tokio::test;

    use crate::{provider::{Data, Provider}, types::UserAgent};

    #[test]
    async fn test_async_operation() {
        // Parse config file
        // let config = ConfigParser::parse_config_file("test_files/config.json").unwrap();

        dotenv().ok();

        let headers = Headers::default();

        let token = env::var("TOKEN").expect("Token is required in .env file!");

        // Create a test channel for communication
        // let (tx, _rx) = tokio::sync::mpsc::channel(100);

        let user_agent_data = Data {
            device_id: Some("13977301-4cfd-4cb4-98b6-3536e0744015".to_string()),
            user_agent: Some(UserAgent::default()),
            ..Default::default()
        };

        let auth_data = Data {
            chats_count: Some(40),
            chats_sync: Some(0),
            contacts_sync: Some(0),
            drafts_sync: Some(0),
            interactive: Some(true),
            presence_sync: Some(-1),
            token: Some(token.to_string()),
            ..Default::default()
        };

        // Initialize the provider with the config and channel
        let _provider = Provider::new(
            serde_json::to_string(&headers).unwrap(),
            "wss://ws-api.oneme.ru/websocket".to_string(),
            user_agent_data,
            auth_data,
        )
        .await
        .unwrap();
    }
}
