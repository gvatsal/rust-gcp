use axum::http::Request;
use tower_http::trace::MakeSpan;
use tracing::{Level, Span};

#[derive(Debug, Clone)]
pub struct CustomMakeSpan {
    level: Level,
    include_headers: bool,
}

impl CustomMakeSpan {
    /// Create a new `CustomMakeSpan`.
    pub fn new() -> Self {
        Self {
            level: Level::DEBUG,
            include_headers: false,
        }
    }

    /// Set the [`Level`] used for the [tracing span].
    ///
    /// Defaults to [`Level::DEBUG`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// Include request headers on the [`Span`].
    ///
    /// By default headers are not included.
    ///
    /// [`Span`]: tracing::Span
    pub fn include_headers(mut self, include_headers: bool) -> Self {
        self.include_headers = include_headers;
        self
    }
}

impl Default for CustomMakeSpan {
    fn default() -> Self {
        Self::new()
    }
}

impl<B> MakeSpan<B> for CustomMakeSpan {
    // This ugly macro is needed, unfortunately, because `tracing::span!`
    // required the level argument to be static. Meaning we can't just pass
    // `self.level`.
    fn make_span(&mut self, request: &Request<B>) -> Span {
        macro_rules! make_span {
            ($level:expr) => {
                {
                    let user_ip = request.headers().get("x-forwarded-for").and_then(|hv| hv.to_str().ok()).unwrap_or("");
                    let user_agent = request.headers().get("user-agent").and_then(|hv| hv.to_str().ok()).unwrap_or("");

                    if self.include_headers {
                        tracing::span!(
                            $level,
                            "request",
                            method = %request.method(),
                            uri = %request.uri(),
                            version = ?request.version(),
                            user_ip = %user_ip,
                            user_agent = %user_agent,
                            headers = ?request.headers(),
                        )
                    } else {
                        tracing::span!(
                            $level,
                            "request",
                            method = %request.method(),
                            uri = %request.uri(),
                            version = ?request.version(),
                            user_ip = %user_ip,
                            user_agent = %user_agent,
                        )
                    }
                }
            }
        }

        match self.level {
            Level::ERROR => make_span!(Level::ERROR),
            Level::WARN => make_span!(Level::WARN),
            Level::INFO => make_span!(Level::INFO),
            Level::DEBUG => make_span!(Level::DEBUG),
            Level::TRACE => make_span!(Level::TRACE),
        }
    }
}