use futures_util::{Stream, StreamExt};
use std::fmt;
use tokio::sync::mpsc;

use super::{LeaseGuard, LeaseLost};

#[derive(Debug)]
pub enum LeaseStreamError<E> {
    Upstream(E),
    LeaseLost(LeaseLost),
}

impl<E: fmt::Display> fmt::Display for LeaseStreamError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Upstream(error) => error.fmt(f),
            Self::LeaseLost(error) => error.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for LeaseStreamError<E> {}

impl LeaseGuard {
    /// Owns the upstream stream in a cancellation-aware task, including while the
    /// downstream is backpressured or never polls again. Drop closes the receiver.
    pub fn stream<S, T, E>(
        self,
        stream: S,
    ) -> impl Stream<Item = Result<T, LeaseStreamError<E>>> + Send
    where
        S: Stream<Item = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let (send, recv) = mpsc::channel(1);
        let mut cancellation = self.cancellation();
        let output_cancellation = cancellation.clone();
        tokio::spawn(async move {
            let guard = self;
            let mut stream = Box::pin(stream);
            loop {
                let item = tokio::select! { biased;
                    () = cancellation.cancelled() => break,
                    () = send.closed() => break,
                    item = stream.next() => item,
                };
                let ended = item.is_none();
                let result = tokio::select! { biased;
                    () = cancellation.cancelled() => break,
                    result = send.send(item.map(|item| item.map_err(LeaseStreamError::Upstream))) => result,
                };
                if result.is_err() {
                    break;
                }
                if ended {
                    tokio::select! { biased;
                        () = cancellation.cancelled() => {},
                        () = send.closed() => {},
                    }
                    break;
                }
            }
            drop(stream);
            drop(guard);
        });
        futures_util::stream::unfold(
            (recv, output_cancellation, false),
            |(mut recv, mut cancellation, ended)| async move {
                if ended {
                    return None;
                }
                tokio::select! { biased;
                    () = cancellation.cancelled() => Some((Err(LeaseStreamError::LeaseLost(LeaseLost)), (recv, cancellation, true))),
                    item = recv.recv() => item.flatten().map(|item| (item, (recv, cancellation, false))),
                }
            },
        )
    }
}
