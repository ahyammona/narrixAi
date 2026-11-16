use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub x: XCfg,
}

#[derive(Debug, Clone, Deserialize)]
pub struct XCfg {
    pub enabled: bool,
    pub woeid: u64,
    pub max_results: u32,
    pub queries: Vec<String>,
    /// "xplore" (logged-in cookie session, no paid API) or "official" (X API v2 bearer token)
    #[serde(default = "default_x_backend")]
    pub backend: String,
    /// "top" (what's performing) or "latest" (what's newest). xplore only.
    #[serde(default = "default_x_search_mode")]
    pub search_mode: String,
    /// Pause between X requests, in seconds. xplore only.
    #[serde(default = "default_x_delay")]
    pub request_delay_secs: u64,
    /// Also pull trending topics.
    #[serde(default = "default_x_trends")]
    pub use_trends: bool,
}

#[derive(Debug, Clone)]
pub struct Secrets {
    pub x_cookie: Option<String>,
}

impl Secrets {
    pub fn from_env() -> Self {
        Secrets {
            x_cookie: std::env::var("X_COOKIE_STRING")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }
}

fn default_x_backend() -> String {
    "xplore".into()
}
fn default_x_search_mode() -> String {
    "top".into()
}
fn default_x_delay() -> u64 {
    3
}
fn default_x_trends() -> bool {
    true
}
