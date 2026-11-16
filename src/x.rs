use xplore::{profile::Profile, Xplore, XploreError};

use crate::error::PumpFunError;

pub async fn search_tweets(
    xplore: &mut Xplore,
    search: &str,
) -> Result<(bool, String, String), PumpFunError> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match xplore
            .search_tweets(search, 3, xplore::search::SearchMode::Top, None)
            .await
        {
            Ok(resp) => {
                if resp.tweets.is_empty() {
                    return Ok((
                        false,
                        String::new(),
                        "No matching posts found on X.".to_string(),
                    ));
                }

                let tweets = resp
                    .tweets
                    .iter()
                    .take(3)
                    .map(|tweet| {
                        let author = tweet
                            .username
                            .as_deref()
                            .map(|username| format!("@{username}"))
                            .or_else(|| tweet.name.clone())
                            .unwrap_or_else(|| "Unknown author".to_string());
                        let text = tweet.text.as_deref().unwrap_or_default();
                        format!("{author}: {text}")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let tweet_ids = resp
                    .tweets
                    .iter()
                    .take(3)
                    .filter_map(|tweet| tweet.id.as_deref())
                    .collect::<Vec<_>>()
                    .join(",");
                return Ok((true, tweet_ids, tweets));
            }
            Err(e) => {
                eprintln!("search attempt {attempt} failed: {e:?}");
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_secs(2 * attempt)).await;
            }
        }
    }
    Err(PumpFunError::Custom(last_err.unwrap().to_string()))
}
