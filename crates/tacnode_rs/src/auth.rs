use anyhow;
use reqwest;
use url;

pub struct AuthClient {
    pub base_url: url::Url,
    pub ops_token: String,
}

impl AuthClient {
    pub fn new(endpoint: String, token: String) -> anyhow::Result<Self> {
        let base_url = url::Url::parse(&endpoint).unwrap();
        Ok(Self {
            base_url,
            ops_token: token,
        })
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<String> {
        let full_url = self.base_url.join(path).unwrap();
        let client = reqwest::Client::new();
        let response = client
            .get(full_url)
            .bearer_auth(&self.ops_token)
            .send()
            .await
            .unwrap();
        let text = response.text().await?;
        Ok(text)
    }
}
