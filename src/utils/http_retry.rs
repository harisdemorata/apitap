use http::Extensions;
use reqwest::{Client, Request, Response};
use reqwest_middleware::{
    ClientBuilder, ClientWithMiddleware, Middleware, Next, Result as MwResult,
};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::time::{Duration, Instant};
use tracing::warn;

#[derive(Debug, Default, Clone)]
struct AttemptCount(pub u32);

struct AttemptLogger;

#[async_trait::async_trait]
impl Middleware for AttemptLogger {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> MwResult<Response> {
        let attempt = match extensions.get_mut::<AttemptCount>() {
            Some(c) => {
                c.0 += 1;
                c.0
            }
            None => {
                extensions.insert(AttemptCount(1));
                1
            }
        };

        let method = req.method().clone();
        let url = req.url().clone();
        let t0 = Instant::now();
        tracing::debug!("→ attempt #{attempt} {method} {url}");

        let res = next.run(req, extensions).await;

        match &res {
            Ok(resp) => {
                let dt = t0.elapsed();
                tracing::debug!(
                    "← attempt #{attempt} {} {} in {:?}",
                    resp.status(),
                    resp.url(),
                    dt
                );
            }
            Err(err) => {
                let dt = t0.elapsed();
                warn!("⇠ attempt #{attempt} error after {:?}: {err}", dt);
            }
        }
        res
    }
}

struct SummaryLogger;

#[async_trait::async_trait]
impl Middleware for SummaryLogger {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> MwResult<Response> {
        let method = req.method().clone();
        let url = req.url().clone();
        let t0 = Instant::now();

        let res = next.run(req, extensions).await;

        let attempts = extensions.get::<AttemptCount>().map(|c| c.0).unwrap_or(1);
        match &res {
            Ok(resp) => {
                tracing::debug!(
                    "✔ {method} {url} -> {} in {:?} (attempts: {attempts})",
                    resp.status(),
                    t0.elapsed()
                );
            }
            Err(err) => {
                warn!(
                    "✖ {method} {url} failed after {:?} (attempts: {attempts}): {err}",
                    t0.elapsed()
                );
            }
        }
        res
    }
}

pub fn build_client_with_retry(
    reqwest_client: Client,
    config_retray: &crate::pipeline::Retry,
) -> ClientWithMiddleware {
    let policy = ExponentialBackoff::builder()
        .retry_bounds(
            Duration::from_secs(config_retray.min_delay_secs),
            Duration::from_secs(config_retray.max_delay_secs),
        )
        .build_with_max_retries(config_retray.max_attempts);

    ClientBuilder::new(reqwest_client)
        .with(AttemptLogger)
        .with(RetryTransientMiddleware::new_with_policy(policy))
        .with(SummaryLogger)
        .build()
}
