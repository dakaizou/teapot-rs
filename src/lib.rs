use std::pin::Pin;

use futures::{Stream, StreamExt};
use pin_project::pin_project;
use tokio_stream::wrappers::BroadcastStream;

#[pin_project]
pub struct Teapot<St, Item>
where
    St: Stream<Item = Item>,
{
    #[pin]
    stream: St,
    publisher: Option<tokio::sync::broadcast::Sender<Item>>,
}

impl<St, Item> Teapot<St, Item>
where
    St: Stream<Item = Item>,
    Item: Clone + Send + 'static,
{
    pub fn new(stream: St, publisher: tokio::sync::broadcast::Sender<Item>) -> Self {
        Self {
            stream,
            publisher: Some(publisher),
        }
    }

    pub fn subscribe(&self) -> Option<BroadcastStream<Item>> {
        self.publisher
            .as_ref()
            .map(|publisher| BroadcastStream::new(publisher.subscribe()))
    }

    pub fn spawn_drain(self)
    where
        St: Send + 'static,
    {
        tokio::spawn(async move {
            self.map(Ok).forward(futures::sink::drain()).await.ok();
        });
    }
}

impl<St, Item> Stream for Teapot<St, Item>
where
    St: Stream<Item = Item>,
    Item: Clone,
{
    type Item = Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        let result = this.stream.as_mut().poll_next(cx);
        match result {
            std::task::Poll::Ready(Some(ref item)) => {
                this.publisher.as_ref().unwrap().send(item.clone()).ok();
            }
            std::task::Poll::Ready(None) => {
                this.publisher.take();
            }
            std::task::Poll::Pending => (),
        }
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

pub fn tea<St, Item>(stream: St, capacity: usize) -> (Teapot<St, Item>, BroadcastStream<Item>)
where
    St: Stream<Item = Item>,
    Item: Clone + Send + 'static,
{
    let (tx, rx) = tokio::sync::broadcast::channel(capacity);
    (Teapot::new(stream, tx), BroadcastStream::new(rx))
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use futures::StreamExt;
    use futures_test::{assert_stream_done, assert_stream_next};

    use super::*;

    const RANGE: Range<i32> = 0..4;

    #[tokio::test]
    async fn it_works() {
        let stream = futures::stream::iter(RANGE);

        let (mut pot, mut sub) = tea(stream, 5);

        for i in RANGE {
            assert_stream_next!(pot, i);
        }
        assert_stream_done!(pot);

        for i in RANGE {
            assert_stream_next!(sub, Ok(i));
        }
        assert_stream_done!(sub);
    }

    #[tokio::test]
    async fn zip() {
        let stream = futures::stream::iter(RANGE);

        let (pot, sub) = tea(stream, 1);
        let mut s = pot.zip(sub);

        for i in RANGE {
            assert_stream_next!(s, (i, Ok(i)));
        }
        assert_stream_done!(s);
    }

    #[tokio::test]
    async fn sub_zip() {
        let stream = futures::stream::iter(0..4);

        let (mut pot, sub1) = tea(stream, 5);
        let sub2 = pot.subscribe().unwrap();

        while let Some(item) = pot.next().await {
            println!("pot found: {item:?}");
        }
        pot.spawn_drain();

        let mut sub = sub1.zip(sub2);
        while let Some(item) = sub.next().await {
            println!("sub found: {item:?}");
        }
    }
}
