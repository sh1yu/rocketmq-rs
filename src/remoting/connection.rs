use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};

use futures::{
    task::{Context, Poll},
    Future, Sink, SinkExt, Stream, StreamExt,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::error::{ConnectionError, Error};
use crate::protocol::{MqCodec, RemotingCommand};

pub struct ConnectionSender {
    addr: String,

    //cmd发送使用的sender
    tx: mpsc::UnboundedSender<RemotingCommand>,

    //需要接收结果时发送receiver对应的sender使用的sender
    //这里有些技巧：我需要一个结果，这里不是直接存储一个能接收到结果的receiver
    //而是临时新创建一个sender_1, receiver_1 对，使用receiver_1进行结果的接收
    //而远端是怎么知道要把网络结果回传到receiver_1，也就是远端怎么知道要把结果发送到sender_1呢？
    //解决方式就是把sender_1和其对应的编号通过registrations_tx发送过去
    registrations_tx: mpsc::UnboundedSender<(i32, oneshot::Sender<RemotingCommand>)>,

    receiver_shutdown: Option<oneshot::Sender<()>>,
    opaque_id: AtomicI32,
}

impl fmt::Debug for ConnectionSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSender")
            .field("addr", &self.addr)
            .finish()
    }
}

impl ConnectionSender {
    pub fn new(
        addr: String,
        tx: mpsc::UnboundedSender<RemotingCommand>,
        registrations_tx: mpsc::UnboundedSender<(i32, oneshot::Sender<RemotingCommand>)>,
        receiver_shutdown: oneshot::Sender<()>,
    ) -> Self {
        Self {
            addr,
            tx,
            registrations_tx,
            receiver_shutdown: Some(receiver_shutdown),
            opaque_id: AtomicI32::new(1),
        }
    }

    #[tracing::instrument(skip(self, cmd))]
    pub async fn send(&self, cmd: RemotingCommand) -> Result<RemotingCommand, Error> {

        //sender也会被发送，在有返回数据产生时使用sender进行往回发送
        let (sender, receiver) = oneshot::channel();
        let mut cmd = cmd;
        cmd.header.opaque = self.opaque_id.fetch_add(1, Ordering::SeqCst);
        debug!(
            code = cmd.code(),
            opaque = cmd.header.opaque,
            cmd = ?cmd, //?是tracing中debug宏的快捷写法，表示该field需要使用fmt::Debug来记录
            "sending remoting command to {}",
            &self.addr
        );
        match (
            // 将sender进行注册：发送给registrations_tx, 并给一个编号opaque
            self.registrations_tx.send((cmd.header.opaque, sender)),
            // 将cmd通过tx发送出去
            self.tx.send(cmd),
        ) {
            // 在registrations_tx发送注册sender成功和tx发送消息成功后，使用sender对应的receiver接收结果，如果有结果的话，返回
            (Ok(_), Ok(_)) => receiver.await.map_err(|_err| Error::Connection(ConnectionError::Disconnected)),
            _ => Err(Error::Connection(ConnectionError::Disconnected)),
        }
    }

    pub async fn send_oneway(&self, cmd: RemotingCommand) -> Result<(), Error> {
        let mut cmd = cmd;
        cmd.header.opaque = self.opaque_id.fetch_add(1, Ordering::SeqCst);
        //oneway只需要发送cmd就行，不用接收结果
        self.tx.send(cmd)
            .map_err(|_| Error::Connection(ConnectionError::Disconnected))?;
        Ok(())
    }
}

struct Receiver<S: Stream<Item=Result<RemotingCommand, Error>>> {
    addr: String,
    inbound: Pin<Box<S>>,
    // internal sender
    // outbound: mpsc::UnboundedSender<RemotingCommand>,
    pending_requests: HashMap<i32, oneshot::Sender<RemotingCommand>>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<(i32, oneshot::Sender<RemotingCommand>)>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
}

impl<S: Stream<Item=Result<RemotingCommand, Error>>> Receiver<S> {
    pub fn new(
        addr: String,
        inbound: S,
        // outbound: mpsc::UnboundedSender<RemotingCommand>,
        registrations: mpsc::UnboundedReceiver<(i32, oneshot::Sender<RemotingCommand>)>,
        shutdown: oneshot::Receiver<()>,
    ) -> Receiver<S> {
        Self {
            addr,
            inbound: Box::pin(inbound),
            // outbound,
            pending_requests: HashMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
        }
    }
}

impl<S: Stream<Item=Result<RemotingCommand, Error>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(ctx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(..)) => {
                return Poll::Ready(Err(()));
            }
            Poll::Pending => {}
        }
        loop {
            match self.registrations.as_mut().poll_recv(ctx) { //registrations是channel，使用poll_recv
                Poll::Ready(Some((opaque, resolver))) => {
                    // 接收到sender通过registrations_tx发送过来的"注册"信息
                    // 将sender_1和唯一标识存起来
                    self.pending_requests.insert(opaque, resolver);
                }
                Poll::Ready(None) => return Poll::Ready(Err(())),
                Poll::Pending => break, //没有事件则break,不要阻塞
            }
        }
        #[allow(clippy::never_loop)]
        loop {
            match self.inbound.as_mut().poll_next(ctx) {//inbound是stream，使用poll_next
                Poll::Ready(Some(Ok(msg))) => {
                    // 接收到了stream回传的命令响应
                    debug!(
                        code = msg.code(),
                        opaque = msg.header.opaque,
                        remark = %msg.header.remark,
                        cmd = ?msg,
                        "received remoting command from {}",
                        &self.addr
                    );
                    if msg.is_response_type() {
                        if let Some(resolver) = self.pending_requests.remove(&msg.header.opaque) {
                            //如果确认是需要响应的，在这里将resp发送出去
                            let _ = resolver.send(msg);
                        }
                    } else {
                        // FIXME: what to do?
                    }
                }
                Poll::Ready(None) => return Poll::Ready(Err(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(_e))) => return Poll::Ready(Err(())),
            }
        }
    }
}

pub struct Connection {
    addr: String,
    sender: ConnectionSender,
}

impl Connection {
    pub async fn new(addr: &str) -> Result<Self, Error> {
        let sender = Connection::prepare_stream(addr.to_string()).await?;
        Ok(Self {
            addr: addr.to_string(),
            sender,
        })
    }

    #[tracing::instrument(name = "connect")]
    async fn prepare_stream(addr: String) -> Result<ConnectionSender, Error> {
        info!("connecting to server");
        let stream = TcpStream::connect(&addr).await
            .map(|stream| tokio_util::codec::Framed::new(stream, MqCodec))?;
        info!("server connected");
        Connection::connect(addr, stream).await
    }

    async fn connect<S>(addr: String, stream: S) -> Result<ConnectionSender, Error>
        where
            S: Stream<Item=Result<RemotingCommand, Error>>,
            S: Sink<RemotingCommand, Error=Error>,
            S: Send + std::marker::Unpin + 'static,
    {
        // 将流分为输入和输出流
        let (mut sink, stream) = stream.split();

        // rx的消息会传给tcp的流，tx则是存储起来，在send等调用时用于输入
        let (tx, mut rx): (mpsc::UnboundedSender<RemotingCommand>, mpsc::UnboundedReceiver<RemotingCommand>) = mpsc::unbounded_channel();

        //registrations_tx会存储起来，用于在send等调用是用于注册
        let (registrations_tx, registrations_rx) = mpsc::unbounded_channel();

        //receiver_shutdown_tx会存储起来，用于sender等主动shutdown
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();

        //接收协程，接收stream来的数据. 在stream有数据时会推动Receiver这个future前进，在配置满足时将数据通过registrations中注册的sender发送出来
        tokio::spawn(Box::pin(Receiver::new(
            addr.clone(),
            stream,
            // tx.clone(),
            registrations_rx,
            receiver_shutdown_rx,
        )));

        // 发送协程，从rx接收发送者信息,将信息流传给tcp的sink
        tokio::spawn(Box::pin(async move {
            while let Some(msg) = rx.recv().await {
                debug!(
                    code = msg.code(),
                    opaque = msg.header.opaque,
                    remark = %msg.header.remark,
                    cmd = ?msg,
                    "connect rx.recv() and sink to  sink ",
                );
                if let Err(_e) = sink.send(msg).await {
                    // FIXME: error handling
                    break;
                }
            }
        }));

        let sender = ConnectionSender::new(addr, tx, registrations_tx, receiver_shutdown_tx);
        Ok(sender)
    }

    pub fn sender(&self) -> &ConnectionSender {
        &self.sender
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            info!("shutting down connection to {}", &self.addr);
            let _ = shutdown.send(());
        }
    }
}
