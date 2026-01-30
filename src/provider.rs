use min_rs_config::UserAgent;
use std::{collections::HashMap, hash::Hash, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc::Sender};

use futures_util::{
    StreamExt,
    sink::SinkExt,
    stream::{SplitSink, SplitStream},
};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        http::Request,
        protocol::Message,
    },
};

use crate::connection_info::ConnectionInfo;

type SplitedStream = SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>;
type SplitedSink = SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>;

type AsyncError = dyn std::error::Error + Send + Sync;

/// A common struct used to send requests to max's reverse-engineered backend
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Data {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interactive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chats_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chats_sync: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drafts_sync: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_sync: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contacts_sync: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<UserAgent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backward: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forward: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_messages: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ids: Option<Vec<String>>,
}

impl Default for Data {
    fn default() -> Self {
        Data {
            interactive: None,
            chats_count: None,
            chats_sync: None,
            drafts_sync: None,
            presence_sync: None,
            contacts_sync: None,
            token: None,
            device_id: None,
            user_agent: None,
            backward: None,
            chat_id: None,
            forward: None,
            from: None,
            get_messages: None,
            message_id: None,
            message_ids: None,
        }
    }
}

/// All the current user's names in max
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MaxSelfNames {
    name: String,
    first_name: String,
    last_name: String,
    #[serde(rename = "type")]
    typ: String,
}

/// Structure that represents current user's data
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MaxSelfContact {
    account_status: i8,
    base_url: String,
    names: Vec<MaxSelfNames>,
    phone: i64,
    options: Vec<String>,
    photo_id: i64,
    update_time: usize,
    id: i64,
    base_raw_url: String,
}

/// Struct used to describe current user's profile options
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MaxSelfProfile {
    profile_options: Vec<i8>,
    contact: MaxSelfContact,
}

/// Struct used to describe max chat option
/// E.g. whether it's  official or not, some participants' privileges etc.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct MaxChatOptions {
    sign_admin: Option<bool>,
    official: Option<bool>,
    message_copy_not_allowed: Option<bool>,
    only_owner_can_change_icon_title: Option<bool>,
    only_admin_can_add_member: Option<bool>,
    only_admin_can_call: Option<bool>,
    sent_by_phone: Option<bool>,
    content_level_chat: Option<bool>,
    a_plus_channel: Option<bool>,
    all_can_pin_message: Option<bool>,
}

/// Struct that represents all last message elements
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LastMessageElement {
    #[serde(rename = "type")]
    typ: String,
    length: i64,
}

/// Struct that represents last message in any chat(DM, GROUP, CHANNEL)
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LastMessage {
    elements: Vec<LastMessageElement>,
    options: i64,
    id: i64,
    time: usize,
    text: String,
    #[serde(rename = "type")]
    typ: String,
}

/// Struct that represents max's chat that user is a participant of
/// Can be used to retrieve all user's chats
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MaxChat {
    participants_count: Option<i64>,
    access: Option<String>,
    invited_by: Option<i64>,
    base_raw_icon_url: Option<String>,
    link: Option<String>,
    description: Option<String>,
    #[serde(rename = "type")]
    typ: String,
    title: Option<String>,
    last_fire_delayed_error_time: i64,
    last_delayed_update_time: i64,
    new_messages: Option<i64>,
    options: Option<MaxChatOptions>,
    modified: usize,
    id: i64,
    owner: i64,
    join_time: usize,
    created: usize,
    restrictions: Option<i64>,
    last_event_time: usize,
    messages_count: Option<i64>,
    base_icon_url: Option<String>,
    status: String,
    cid: Option<i64>,
}

/// Struct used to describe many max user's names due to max names system
/// Each user can have their official name(As I understand it gets it from GosUslugi)
/// and also their custom name
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ContactName {
    name: String,
    #[serde(rename = "type")]
    typ: String,
    first_name: String,
}

/// Struct that represents max user's contact
/// Can be user to get list of user's contacts
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Contact {
    account_status: i64,
    names: Vec<ContactName>,
    update_time: usize,
    id: i64,
}

/// Structure that describes some of max's message fields
/// Can be user to represent message or many messages and interact with them
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MaxMessage {
    id: String,
    sender: Option<i64>,
    text: String,
    time: usize,
    #[serde(rename = "type")]
    typ: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum MaybeEmpty<T> {
    Full(T),
    Empty {},
}

/// This is a common struct used to parse max responses
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MaxResponse {
    video_chat_history: Option<bool>,
    profile: Option<MaxSelfProfile>,
    chats: Option<Vec<MaxChat>>,
    contacts: Option<Vec<Contact>>,
    messages: Option<MaybeEmpty<Vec<MaxMessage>>>,
    message: Option<MaxMessage>,
    chat_id: Option<i64>,
}

/// Struct used to get max's response with payload
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResponseState {
    cmd: i8,
    opcode: i64,
    seq: i64,
    ver: i8,
    pub payload: MaxResponse,
}

/// Struct used to get max's response without payload
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResponseHeaders {
    cmd: i8,
    opcode: i64,
    seq: i64,
    ver: i8,
}

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

/// Provider is the main struct to work with max's reverse engineered api
/// Here is an example of usage:
/// '''
///
/// '''
pub struct Provider {
    read: Arc<Mutex<SplitedStream>>,
    write: Arc<Mutex<SplitedSink>>,
    headers: String,
    uri: String,
    state: RequestState,
    full_data: Option<ResponseState>,
    tx: Sender<String>,
    user_agent: Data,
    auth_data: Data,
    connection_info: ConnectionInfo,
    named_identifiers: HashMap<i64, String>,
}

impl Provider {
    pub async fn new(
        headers: String,
        uri: String,
        tx: Sender<String>,
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
            tx,
            headers,
            uri,
            user_agent,
            auth_data,
            connection_info: ConnectionInfo::new(),
            named_identifiers: HashMap::new(),
        })
    }

    pub async fn auth(&mut self) -> Result<(), Box<AsyncError>> {
        println!(
            "Authenticating with data: {:#?}\n{:#?}",
            self.user_agent, self.auth_data
        );
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
        println!("Seq: {}", state_copy.seq);
        let raw_data = serde_json::to_string(&state_copy)?;
        match self.write_to_stream(raw_data).await {
            Err(e) => {
                println!(
                    "Failed to write into stream: {}. Initializing new session...",
                    e
                );
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
            Err(e) => {
                println!("Reconnection failed: {}", e);
            }
        }

        let stream = connect_to_servers(self.headers.clone(), self.uri.clone())
            .await
            .unwrap();

        println!("Closing old connection...");
        self.write.lock().await.close().await?;
        println!("Old connection closed. Establishing new one...");

        let (write, read) = stream.split();

        self.write = Arc::new(Mutex::new(write));
        self.read = Arc::new(Mutex::new(read));

        self.auth().await?;

        Ok(())
    }

    pub async fn handle_everything(&mut self) -> Result<(), Box<AsyncError>> {
        let mut interaction_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                result = self.handle_messages() => {
                    result?;
                }
                _ = interaction_interval.tick() => {
                    self.accept_interactions().await?;
                }
            }
        }
    }

    async fn handle_messages(&mut self) -> Result<(), Box<AsyncError>> {
        let read_clone = Arc::clone(&self.read);
        let text = match {
            let mut guard = read_clone.lock().await;
            guard.next().await
        } {
            Some(Ok(Message::Text(text))) => {
                println!("Read from stream: {}", text);
                text
            }
            Some(Err(e)) => {
                println!("Error: {}. Initializing new session...", e);
                self.init_new_session().await?;
                return Ok(());
            }
            Some(_) => {
                println!("Got something weird");
                return Ok(());
            }
            None => {
                println!("Stream is closed!");
                return Ok(());
            }
        };
        let headers: ResponseHeaders = serde_json::from_str(&text).unwrap();
        if headers.opcode == 19 {
            println!("{}", &text);
            let response: ResponseState = serde_json::from_str(&text).unwrap();
            self.full_data = Some(response.clone());
            for chat in response.payload.chats.unwrap().iter() {
                if let Some(title) = &chat.title {
                    self.named_identifiers.insert(chat.id, title.to_string());
                }
            }
            for contact in response.payload.contacts.unwrap().iter() {
                self.named_identifiers
                    .insert(contact.id, contact.names[0].name.clone());
            }
            println!("{:#?}", self.named_identifiers);
        }
        if headers.opcode == 49 {
            let response: ResponseState = serde_json::from_str(&text).unwrap();
            if let Some(maybe_empty) = &response.payload.messages {
                match maybe_empty {
                    MaybeEmpty::Full(msgs) => {
                        for message in msgs {
                            let tg_text = format!(
                                "Author: {}\nText: {}",
                                message.sender.unwrap(),
                                message.text
                            );
                            self.tx.send(tg_text).await.unwrap();
                        }
                    }
                    MaybeEmpty::Empty {} => {
                        println!("Empty message encountered");
                    }
                }
            }
        }
        if headers.opcode == 128 {
            let data: ResponseState = serde_json::from_str(&text).unwrap();
            
            let id = match data.payload.message.clone().unwrap().sender {
                Some(id) => id,
                None => data.payload.chat_id.unwrap()
            };

            println!(
                "Answer to message:\nChat id: {}\nMsg id: {}",
                id,
                data.payload.message.clone().unwrap().id
            );

            let name = self.get_name_by_id(id);

            let tg_text = format!("{}\n\n{}", name, data.payload.message.clone().unwrap().text);

            self.tx.send(tg_text).await.unwrap();

            // Answer to max that we have received the message
            self.send_data(
                Data {
                    chat_id: data.payload.chat_id,
                    message_id: Some(data.payload.message.clone().unwrap().id),
                    ..Default::default()
                },
                128,
                1,
            )
            .await
            .unwrap();

            let mut msg_ids = Vec::new();
            msg_ids.push(data.payload.message.unwrap().id);

            self.send_data(
                Data {
                    chat_id: data.payload.chat_id,
                    message_ids: Some(msg_ids),
                    ..Default::default()
                },
                74,
                0,
            )
            .await
            .unwrap();
        }

        // while let Some(Ok(Message::Text(text))) = read_clone.lock().await.next().await {}

        Ok(())
    }

    fn get_name_by_id(&self, id: i64) -> String {
        let result = self.named_identifiers.get(&id).unwrap().to_string();

        result
    }

    async fn accept_interactions(&mut self) -> Result<(), Box<AsyncError>> {
        println!("Sending interactable true");
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

    use crate::provider::{Data, Provider};

    #[test]
    async fn test_async_operation() {
        // Parse config file
        let config = ConfigParser::parse_config_file("test_files/config.json").unwrap();

        dotenv().ok();

        let token = env::var("TOKEN").expect("Token is required in .env file!");

        // Create a test channel for communication
        let (tx, _rx) = tokio::sync::mpsc::channel(100);

        let user_agent_data = Data {
            device_id: Some("13977301-4cfd-4cb4-98b6-3536e0744015".to_string()),
            user_agent: Some(config.max_agent),
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
            serde_json::to_string(&config.headers).unwrap(),
            "wss://ws-api.oneme.ru/websocket".to_string(),
            tx,
            user_agent_data,
            auth_data,
        )
        .await
        .unwrap();
    }
}
