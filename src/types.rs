use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserAgent {
    pub app_version: String,
    pub device_locale: String,
    pub device_name: String,
    pub device_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_user_agent: Option<String>,
    pub locale: String,
    pub os_version: String,
    pub screen: String,
    pub timezone: String,
}

impl Default for UserAgent {
    fn default() -> Self {
        Self {
            app_version: "26.4.3".to_string(),
            device_locale: "en".to_string(),
            device_name: "Chrome".to_string(),
            device_type: "WEB".to_string(),
            locale: "ru".to_string(),
            os_version: "Windows".to_string(),
            screen: "1920x1080 1.0x".to_string(),
            timezone: "Europe/Moscow".to_string(),
            header_user_agent: Some("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36".to_string())
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct Headers {
    host: String,
    #[serde(rename = "Accept-Encoding")]
    accept_encoding: String,
    #[serde(rename = "Accept-Language")]
    accept_language: String,
    connection: String,
    origin: String,
    pragma: String,
    #[serde(rename = "Sec-Websocket-Extension")]
    sec_websocket_extension: String,
    #[serde(rename = "Sec-Websocket-Key")]
    sec_websocket_key: String,
    #[serde(rename = "Sec-Websocket-Version")]
    sec_websocket_version: String,
    upgrade: String,
    #[serde(rename = "User-Agent")]
    user_agent: String,
}

impl Default for Headers {
    fn default() -> Self {
        let agent = match UserAgent::default().header_user_agent {
            Some(a) => a.to_string(),
            None => "".to_string()
        };

        Self {
            host: "ws-api.oneme.ru".to_string(),
            accept_encoding: "gzip, deflate, br, zstd".to_string(),
            accept_language: "en-US,en;q=0.9".to_string(),
            connection: "Upgrade".to_string(),
            origin: "https://web.max.ru".to_string(),
            pragma: "no-cache".to_string(),
            sec_websocket_extension: "permessage-deflate; client_max_window_bits".to_string(),
            sec_websocket_key: "MEBa2ZnucwlWNZrrLRbmIQ".to_string(),
            sec_websocket_version: "13".to_string(),
            upgrade: "websocket".to_string(),
            user_agent: agent,
        }
    }
}

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
    pub title: Option<String>,
    last_fire_delayed_error_time: i64,
    last_delayed_update_time: i64,
    new_messages: Option<i64>,
    options: Option<MaxChatOptions>,
    modified: usize,
    pub id: i64,
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
    pub name: String,
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
    pub names: Vec<ContactName>,
    update_time: usize,
    pub id: i64,
}

/// Structure that describes some of max's message fields
/// Can be user to represent message or many messages and interact with them
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MaxMessage {
    pub id: String,
    pub sender: Option<i64>,
    #[serde(skip)]
    pub sender_name: Option<String>,
    pub text: String,
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
    pub chats: Option<Vec<MaxChat>>,
    pub contacts: Option<Vec<Contact>>,
    pub messages: Option<MaybeEmpty<Vec<MaxMessage>>>,
    pub message: Option<MaxMessage>,
    pub chat_id: Option<i64>,
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
    pub opcode: i64,
    seq: i64,
    ver: i8,
}

impl std::fmt::Display for ResponseHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CMD: {}\nOPCODE: {}\nSEQUENCE: {}\nVERSION: {}",
            self.cmd, self.opcode, self.seq, self.ver
        )
    }
}
