//! Native rust_cast backend. Scheduling and DTO conversion live here; Cast wire
//! commands, framing, and response parsing are exclusively provided by rust_cast.

use std::{
    cell::{Cell, RefCell},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    rc::Rc,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, ensure, Context, Result};
use rust_cast::{
    channels::{
        connection::{ConnectionChannel, ConnectionResponse},
        heartbeat::{HeartbeatChannel, HeartbeatResponse},
        media::{self, MediaChannel, MediaResponse},
        receiver::{self, CastDeviceApp, ReceiverChannel, ReceiverResponse},
    },
    errors::Error as CastError,
    message_manager::MessageManager,
};
use rustls::{pki_types::ServerName, ClientConnection, StreamOwned};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};

use crate::{
    command::{Command, MediaLoad, NativeCommand},
    discovery::Device,
};

const RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
const MEDIA: &str = "urn:x-cast:com.google.cast.media";
const SENDER: &str = "sender-0";
const PLATFORM: &str = "receiver-0";
const IDLE_READ: Duration = Duration::from_millis(250);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub enum Event {
    Update {
        device: String,
        namespace: String,
        payload: Value,
    },
    Connection {
        device: String,
        status: &'static str,
    },
}

pub fn spawn(
    device: Device,
    events: mpsc::Sender<Event>,
    shutdown: watch::Receiver<bool>,
) -> mpsc::Sender<Command> {
    let (sender, commands) = mpsc::channel(32);
    let error_events = events.clone();
    let name = device.name.clone();
    let thread_name = format!("cast-{}", device.name);
    if let Err(error) = thread::Builder::new().name(thread_name).spawn(move || {
        let control = Control {
            commands: Rc::new(RefCell::new(commands)),
            shutdown,
            events,
            name: device.name.clone(),
        };
        worker(device.address, control);
    }) {
        log::error!("Cannot start Cast worker for {name}: {error}");
        let _ = error_events.try_send(Event::Connection {
            device: name,
            status: "ERROR",
        });
    }
    sender
}

#[derive(Clone)]
struct Control {
    commands: Rc<RefCell<mpsc::Receiver<Command>>>,
    shutdown: watch::Receiver<bool>,
    events: mpsc::Sender<Event>,
    name: String,
}

impl Control {
    fn stopped(&self) -> bool {
        *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
            || self.commands.borrow().is_closed()
            || self.events.is_closed()
    }

    fn emit(&self, mut event: Event) -> Result<()> {
        // Never block indefinitely in blocking_send: retirement must still work
        // when the async consumer is backpressured or shutting down.
        loop {
            ensure!(!self.stopped(), "Cast worker stopped");
            match self.events.try_send(event) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    event = returned;
                    thread::sleep(Duration::from_millis(20));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => bail!("Cast event consumer closed"),
            }
        }
    }

    fn status(&self, status: &'static str) -> Result<()> {
        self.emit(Event::Connection {
            device: self.name.clone(),
            status,
        })
    }

    fn update(&self, namespace: &str, payload: Value) -> Result<()> {
        self.emit(Event::Update {
            device: self.name.clone(),
            namespace: namespace.to_owned(),
            payload,
        })
    }

    fn error(&self, error: &anyhow::Error) -> Result<()> {
        log::warn!("Cast {}: {error:#}", self.name);
        self.status("ERROR")
    }

    fn wait(&self, duration: Duration) {
        let deadline = Instant::now() + duration;
        while !self.stopped() && Instant::now() < deadline {
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50)),
            );
        }
    }
}

fn worker(address: SocketAddr, control: Control) {
    let mut backoff = Duration::from_secs(1);
    while !control.stopped() {
        if control.status("CONNECTING").is_err() {
            break;
        }
        let started = Instant::now();
        // A malformed response can panic inside upstream synchronous methods.
        // Retire that session and reconnect instead of leaving a dead actor.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Session::connect(address, control.clone()).and_then(|mut session| session.run())
        }))
        .unwrap_or_else(|_| Err(anyhow!("rust_cast session panicked")));
        if control.stopped() {
            break;
        }
        if let Err(error) = result {
            log::warn!("Cast {} disconnected: {error:#}", control.name);
        }
        if control.status("DISCONNECTED").is_err() {
            break;
        }
        if started.elapsed() >= Duration::from_secs(30) {
            backoff = Duration::from_secs(1);
        }
        control.wait(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
    // Closing the socket is sufficient. Never send media STOP on worker retirement.
    let _ = control.events.try_send(Event::Connection {
        device: control.name.clone(),
        status: "DISCONNECTED",
    });
}

#[derive(Clone, Copy)]
enum BudgetPhase {
    Operation,
    Idle,
    Frame,
}

struct Budget {
    deadline: Cell<Instant>,
    bytes_read: Cell<usize>,
    phase: Cell<BudgetPhase>,
    control: Control,
}

impl Budget {
    fn begin(&self, duration: Duration) {
        self.deadline.set(Instant::now() + duration);
        self.bytes_read.set(0);
        self.phase.set(BudgetPhase::Operation);
    }

    fn begin_receive(&self) {
        self.begin(IDLE_READ);
        self.phase.set(BudgetPhase::Idle);
    }

    fn record_read(&self, count: usize) {
        if count > 0 && matches!(self.phase.get(), BudgetPhase::Idle) {
            // A frame can start at the end of the idle poll. Give its remainder
            // a full bounded receive budget, not whatever is left of 250 ms.
            self.deadline.set(Instant::now() + OPERATION_TIMEOUT);
            self.phase.set(BudgetPhase::Frame);
        }
        self.bytes_read
            .set(self.bytes_read.get().saturating_add(count));
    }

    fn remaining(&self) -> io::Result<Duration> {
        if self.control.stopped() {
            return Err(io::Error::new(
                // read_exact retries Interrupted forever; retirement must escape it.
                io::ErrorKind::ConnectionAborted,
                "Cast worker stopped",
            ));
        }
        self.deadline
            .get()
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| {
                let phase = match self.phase.get() {
                    BudgetPhase::Operation => "operation",
                    BudgetPhase::Idle => "idle receive",
                    BudgetPhase::Frame => "frame receive",
                };
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "Cast {phase} deadline exceeded ({} plaintext bytes read)",
                        self.bytes_read.get(),
                    ),
                )
            })
    }
}

/// Enforce the same absolute budget inside Rustls's potentially repeated TLS I/O.
struct TimedSocket {
    socket: TcpStream,
    budget: Rc<Budget>,
}

impl Read for TimedSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            self.socket
                .set_read_timeout(Some(self.budget.remaining()?.min(IDLE_READ)))?;
            match self.socket.read(buffer) {
                Err(error) if is_timeout(&error) => continue,
                result => return result,
            }
        }
    }
}

impl Write for TimedSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        loop {
            self.socket
                .set_write_timeout(Some(self.budget.remaining()?.min(IDLE_READ)))?;
            match self.socket.write(buffer) {
                Err(error) if is_timeout(&error) => continue,
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.budget.remaining()?;
        self.socket.flush()
    }
}

/// Count plaintext Cast bytes, not TLS records: Rustls retains incomplete TLS
/// records itself, whereas MessageManager's partial Cast frame buffer is local.
struct TrackedStream<S = StreamOwned<ClientConnection, TimedSocket>> {
    stream: S,
    budget: Rc<Budget>,
}

impl<S: Read> Read for TrackedStream<S> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.budget.remaining()?;
        let count = self.stream.read(buffer)?;
        self.budget.record_read(count);
        Ok(count)
    }
}

impl<S: Write> Write for TrackedStream<S> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.budget.remaining()?;
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.budget.remaining()?;
        self.stream.flush()
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn idle_timeout(error: &CastError, bytes_read: usize) -> bool {
    bytes_read == 0 && matches!(error,CastError::Io(error) if is_timeout(error))
}

fn fatal(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.is::<io::Error>()
            || matches!(
                cause.downcast_ref::<CastError>(),
                Some(CastError::Io(_) | CastError::Tls(_) | CastError::Protobuf(_))
            )
    })
}

struct Session<S: Read + Write = StreamOwned<ClientConnection, TimedSocket>> {
    control: Control,
    budget: Rc<Budget>,
    manager: Rc<MessageManager<TrackedStream<S>>>,
    connection: ConnectionChannel<'static, TrackedStream<S>>,
    heartbeat: HeartbeatChannel<'static, TrackedStream<S>>,
    receiver: ReceiverChannel<'static, TrackedStream<S>>,
    media: MediaChannel<'static, TrackedStream<S>>,
    application: Option<receiver::Application>,
    receiver_status: Option<receiver::Status>,
    media_id: Option<i32>,
    pending_ping: Option<Instant>,
}

impl Session {
    fn connect(address: SocketAddr, control: Control) -> Result<Self> {
        let socket = TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .with_context(|| format!("connecting to {address}"))?;
        socket.set_nodelay(true)?;
        let budget = Rc::new(Budget {
            deadline: Cell::new(Instant::now()),
            bytes_read: Cell::new(0),
            phase: Cell::new(BudgetPhase::Operation),
            control: control.clone(),
        });
        budget.begin(OPERATION_TIMEOUT);
        let mut socket = TimedSocket {
            socket,
            budget: budget.clone(),
        };
        let server_name = ServerName::IpAddress(address.ip().into());
        let mut tls = ClientConnection::new(Arc::new(crate::tls::insecure_config()), server_name)?;
        while tls.is_handshaking() {
            tls.complete_io(&mut socket)?;
        }
        let stream = TrackedStream {
            stream: StreamOwned::new(tls, socket),
            budget: budget.clone(),
        };
        let manager = Rc::new(MessageManager::new(stream));
        Ok(Self {
            control,
            budget,
            manager: manager.clone(),
            connection: ConnectionChannel::new(SENDER, manager.clone()),
            heartbeat: HeartbeatChannel::new(SENDER, PLATFORM, manager.clone()),
            receiver: ReceiverChannel::new(SENDER, PLATFORM, manager.clone()),
            media: MediaChannel::new(SENDER, manager),
            application: None,
            receiver_status: None,
            media_id: None,
            pending_ping: None,
        })
    }
}

impl<S: Read + Write> Session<S> {
    fn handle(&self, result: Result<()>) -> Result<()> {
        match result {
            Err(error) if fatal(&error) => Err(error),
            Err(error) => self.control.error(&error),
            Ok(()) => Ok(()),
        }
    }

    fn run(&mut self) -> Result<()> {
        self.budget.begin(OPERATION_TIMEOUT);
        self.connection.connect(PLATFORM)?;
        self.control.status("CONNECTED")?;
        let initial = self.poll().context("initial status refresh");
        self.handle(initial)?;
        let mut next_ping = Instant::now();
        let mut next_poll = Instant::now() + POLL_INTERVAL;
        while !self.control.stopped() {
            let command = self.control.commands.borrow_mut().try_recv();
            match command {
                Ok(command) => {
                    self.budget.begin(OPERATION_TIMEOUT);
                    let result = self.execute(command).context("executing Cast command");
                    self.handle(result)?;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            let now = Instant::now();

            if now >= next_ping {
                self.budget.begin(OPERATION_TIMEOUT);
                self.ping().context("sending Cast heartbeat")?;
                next_ping = Instant::now() + HEARTBEAT_INTERVAL;
            }
            if now >= next_poll {
                self.budget.begin(OPERATION_TIMEOUT);
                let result = self.poll().context("periodic status refresh");
                self.handle(result)?;
                next_poll = Instant::now() + POLL_INTERVAL;
            }
            self.budget.begin_receive();
            match self.manager.receive() {
                Ok(message) => {
                    let read_from_socket = self.budget.bytes_read.get() != 0;
                    self.budget.begin(OPERATION_TIMEOUT);
                    let result = if self.heartbeat.can_handle(&message) {
                        match self.heartbeat.parse(&message) {
                            Ok(HeartbeatResponse::Ping) => {
                                self.heartbeat.pong().map_err(Into::into)
                            }
                            Ok(HeartbeatResponse::Pong) => {
                                self.pending_ping = None;
                                Ok(())
                            }
                            Ok(_) => Ok(()),
                            Err(error) => Err(error.into()),
                        }
                    } else if self.connection.can_handle(&message) {
                        match self.connection.parse(&message) {
                            Ok(ConnectionResponse::Close) => {
                                return Err(anyhow!(io::Error::new(
                                    io::ErrorKind::ConnectionAborted,
                                    "Cast virtual connection closed"
                                )));
                            }
                            Ok(_) => Ok(()),
                            Err(error) => Err(error.into()),
                        }
                    } else if self.receiver.can_handle(&message) {
                        match self.receiver.parse(&message) {
                            Ok(ReceiverResponse::Status(status)) => self.apply_receiver(status),
                            Ok(other) => {
                                self.control.error(&anyhow!("receiver response: {other:?}"))
                            }
                            Err(error) => Err(error.into()),
                        }
                    } else if self.media.can_handle(&message) {
                        if !self
                            .application
                            .as_ref()
                            .is_some_and(|app| app.transport_id == message.source)
                        {
                            continue;
                        }
                        match self.media.parse(&message) {
                            Ok(MediaResponse::Status(status)) => self.apply_media(status),
                            Ok(other) => self.control.error(&anyhow!("media response: {other:?}")),
                            Err(error) => Err(error.into()),
                        }
                    } else {
                        Ok(())
                    };
                    self.handle(result)?;
                    // Synchronous rust_cast calls can buffer PONGs. Only judge
                    // the deadline after the buffered messages have been drained.
                    if read_from_socket {
                        self.check_heartbeat()?;
                    }
                }
                Err(error) if idle_timeout(&error, self.budget.bytes_read.get()) => {
                    self.check_heartbeat()?;
                }
                Err(error) => {
                    self.handle(Err(anyhow::Error::from(error)).context("receiving Cast frame"))?
                }
            }
        }
        Ok(())
    }

    fn check_heartbeat(&self) -> Result<()> {
        if self
            .pending_ping
            .is_some_and(|sent| sent.elapsed() >= HEARTBEAT_TIMEOUT)
        {
            bail!(io::Error::new(
                io::ErrorKind::TimedOut,
                "Cast heartbeat timeout"
            ));
        }
        Ok(())
    }

    fn ping(&mut self) -> Result<()> {
        self.heartbeat.ping()?;
        self.pending_ping.get_or_insert(Instant::now());
        Ok(())
    }

    fn poll(&mut self) -> Result<()> {
        let status = self
            .receiver
            .get_status()
            .context("requesting receiver status")?;
        let changed = self
            .application
            .as_ref()
            .map(|app| (app.transport_id.clone(), app.session_id.clone()));
        self.apply_receiver(status)
            .context("applying receiver status and attaching media app")?;
        // Attaching already fetched media status once.
        if self
            .application
            .as_ref()
            .map(|app| (app.transport_id.clone(), app.session_id.clone()))
            == changed
            && let Some(app) = self.application.clone()
        {
            let status = self
                .media
                .get_status(app.transport_id.clone(), None)
                .with_context(|| format!("requesting media status from {}", app.transport_id))?;
            self.apply_media(status)?;
        }
        Ok(())
    }

    fn apply_receiver(&mut self, mut status: receiver::Status) -> Result<()> {
        if let Some(previous) = &self.receiver_status {
            status.volume.level = status.volume.level.or(previous.volume.level);
            status.volume.muted = status.volume.muted.or(previous.volume.muted);
        }
        self.control
            .update(RECEIVER, receiver_status_payload(&status))?;
        let application = status
            .applications
            .iter()
            .filter(|app| app.namespaces.iter().any(|namespace| namespace == MEDIA))
            .find(|app| {
                self.application.as_ref().is_some_and(|old| {
                    old.transport_id == app.transport_id && old.session_id == app.session_id
                })
            })
            .or_else(|| {
                status
                    .applications
                    .iter()
                    .find(|app| app.namespaces.iter().any(|namespace| namespace == MEDIA))
            })
            .cloned();
        self.receiver_status = Some(status);
        self.attach(application)
    }

    fn attach(&mut self, application: Option<receiver::Application>) -> Result<()> {
        let unchanged = match (&self.application, &application) {
            (None, None) => true,
            (Some(old), Some(new)) => {
                old.transport_id == new.transport_id && old.session_id == new.session_id
            }
            _ => false,
        };
        if unchanged && application.is_some() {
            self.application = application;
            return Ok(());
        }
        self.media_id = None;
        self.application = application;
        self.control
            .update(MEDIA, json!({"type":"MEDIA_STATUS","status":[]}))?;
        if let Some(app) = self.application.clone() {
            self.connection
                .connect(app.transport_id.clone())
                .with_context(|| format!("connecting media channel {}", app.transport_id))?;
            let status = self
                .media
                .get_status(app.transport_id.clone(), None)
                .with_context(|| {
                    format!("requesting initial media status from {}", app.transport_id)
                })?;
            self.apply_media(status)?;
        }
        Ok(())
    }

    fn forget_stopped_app(&mut self, session_id: &str) -> Result<()> {
        if self
            .application
            .as_ref()
            .is_some_and(|app| app.session_id == session_id)
        {
            self.application = None;
            self.media_id = None;
            self.control
                .update(MEDIA, json!({"type":"MEDIA_STATUS","status":[]}))?;
        }
        if let Some(status) = &mut self.receiver_status {
            status
                .applications
                .retain(|app| app.session_id != session_id);
            self.control
                .update(RECEIVER, receiver_status_payload(status))?;
        }
        Ok(())
    }

    fn apply_media(&mut self, status: media::Status) -> Result<()> {
        let media_id = status.entries.first().map(|entry| entry.media_session_id);
        if self.media_id.is_some() && media_id.is_some() && self.media_id != media_id {
            self.control
                .update(MEDIA, json!({"type":"MEDIA_STATUS","status":[]}))?;
        }
        self.media_id = media_id;
        self.control.update(MEDIA, media_status_payload(&status))
    }

    fn application(&mut self, launch: bool) -> Result<receiver::Application> {
        if self.application.is_none() {
            let status = self.receiver.get_status()?;
            self.apply_receiver(status)?;
        }
        if self.application.is_none() && launch {
            let app = self
                .receiver
                .launch_app(&CastDeviceApp::DefaultMediaReceiver)?;
            ensure!(
                app.namespaces.iter().any(|namespace| namespace == MEDIA),
                "launched app has no media namespace"
            );
            self.attach(Some(app))?;
            let status = self.receiver.get_status()?;
            self.apply_receiver(status)?;
        }
        self.application
            .clone()
            .ok_or_else(|| anyhow!("no media-capable Cast application"))
    }

    fn media_session(&mut self) -> Result<(receiver::Application, i32)> {
        let app = self.application(false)?;
        if self.media_id.is_none() {
            let status = self.media.get_status(app.transport_id.clone(), None)?;
            self.apply_media(status)?;
        }
        Ok((
            app,
            self.media_id
                .ok_or_else(|| anyhow!("no active Cast media session"))?,
        ))
    }

    fn volume(&mut self, volume: receiver::Volume) -> Result<()> {
        let update = self.receiver.set_volume(volume)?;
        if let Some(status) = &mut self.receiver_status {
            status.volume.level = update.level.or(status.volume.level);
            status.volume.muted = update.muted.or(status.volume.muted);
            self.control
                .update(RECEIVER, receiver_status_payload(status))?;
        } else {
            let status = self.receiver.get_status()?;
            self.apply_receiver(status)?;
        }
        Ok(())
    }

    fn load(&mut self, media: media::Media, options: media::LoadOptions) -> Result<()> {
        let app = self.application(true)?;
        let status =
            self.media
                .load_with_opts(app.transport_id, app.session_id, &media, options)?;
        self.apply_media(status)
    }

    fn execute(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Volume { value, relative } => {
                ensure!(value.is_finite(), "volume must be finite");
                let current = if relative {
                    if self
                        .receiver_status
                        .as_ref()
                        .and_then(|status| status.volume.level)
                        .is_none()
                    {
                        let status = self.receiver.get_status()?;
                        self.apply_receiver(status)?;
                    }
                    self.receiver_status
                        .as_ref()
                        .and_then(|status| status.volume.level)
                        .ok_or_else(|| anyhow!("receiver volume is unknown"))?
                        as f64
                } else {
                    0.0
                };
                self.volume(receiver::Volume {
                    level: Some((current + value / 100.0).clamp(0.0, 1.0) as f32),
                    muted: None,
                })
            }
            Command::Mute(muted) => self.volume(receiver::Volume {
                level: None,
                muted: Some(muted),
            }),
            Command::Seek(time) => self.native(NativeCommand::Seek {
                current_time: Some(time),
                resume_state: Some("PLAYBACK_START".into()),
            }),
            Command::Resume => self.native(NativeCommand::Play),
            Command::Pause => self.native(NativeCommand::Pause),
            Command::Stop => self.native(NativeCommand::Stop),
            Command::Load(load) => {
                let (media, options) = legacy_load(*load)?;
                self.load(media, options)
            }
            Command::Native(command) => self.native(command),
        }
    }

    fn native(&mut self, command: NativeCommand) -> Result<()> {
        match command {
            NativeCommand::GetReceiverStatus => {
                let status = self.receiver.get_status()?;
                self.apply_receiver(status)
            }
            NativeCommand::LaunchApp { app_id } => {
                ensure!(!app_id.trim().is_empty(), "app_id is empty");
                let app =
                    CastDeviceApp::from_str(&app_id).map_err(|_| anyhow!("invalid app ID"))?;
                self.receiver.launch_app(&app)?;
                let status = self.receiver.get_status()?;
                self.apply_receiver(status)
            }
            NativeCommand::StopApp { session_id } => {
                let id = session_id
                    .or_else(|| self.application.as_ref().map(|app| app.session_id.clone()))
                    .or_else(|| {
                        self.receiver_status
                            .as_ref()?
                            .applications
                            .first()
                            .map(|app| app.session_id.clone())
                    })
                    .ok_or_else(|| anyhow!("no receiver app session to stop"))?;
                self.receiver.stop_app(id.clone())?;
                self.forget_stopped_app(&id)?;
                let status = self.receiver.get_status()?;
                self.apply_receiver(status)
            }
            NativeCommand::SetVolume { level, muted } => {
                ensure!(
                    level.is_none_or(|level| level.is_finite() && (0.0..=1.0).contains(&level)),
                    "volume level must be in 0..=1"
                );
                self.volume(receiver::Volume { level, muted })
            }
            NativeCommand::BroadcastMessage { namespace, message } => {
                self.receiver.broadcast_message(&namespace, &message)?;
                Ok(())
            }
            NativeCommand::GetMediaStatus => {
                let app = self.application(false)?;
                let status = self.media.get_status(app.transport_id, None)?;
                self.apply_media(status)
            }
            NativeCommand::Load { media, options } => {
                let media = parse_media(&media)?;
                let options = options.as_ref().map(parse_load_options).transpose()?;
                let app = self.application(true)?;
                let status = if let Some(options) = options {
                    self.media
                        .load_with_opts(app.transport_id, app.session_id, &media, options)?
                } else {
                    self.media.load(app.transport_id, app.session_id, &media)?
                };
                self.apply_media(status)
            }
            NativeCommand::LoadQueue { queue } => {
                let queue = parse_queue(&queue)?;
                let app = self.application(true)?;
                let status = self
                    .media
                    .load_queue(app.transport_id, app.session_id, &queue)?;
                self.apply_media(status)
            }
            NativeCommand::LoadWithQueue {
                media,
                queue,
                options,
            } => {
                let media = parse_media(&media)?;
                let queue = parse_queue(&queue)?;
                let options = options
                    .as_ref()
                    .map(parse_load_options)
                    .transpose()?
                    .unwrap_or_default();
                let app = self.application(true)?;
                let status = self.media.load_with_queue(
                    app.transport_id,
                    app.session_id,
                    &media,
                    Some(&queue),
                    options,
                )?;
                self.apply_media(status)
            }
            NativeCommand::Play | NativeCommand::Pause | NativeCommand::Stop => {
                let (app, id) = self.media_session()?;
                let entry = match command {
                    NativeCommand::Play => self.media.play(app.transport_id, id)?,
                    NativeCommand::Pause => self.media.pause(app.transport_id, id)?,
                    NativeCommand::Stop => self.media.stop(app.transport_id, id)?,
                    _ => unreachable!(),
                };
                self.apply_media(media::Status {
                    request_id: 0,
                    entries: vec![entry],
                })
            }
            NativeCommand::Seek {
                current_time,
                resume_state,
            } => {
                let time = current_time
                    .map(|time| nonnegative_f32(time, "current_time"))
                    .transpose()?;
                let resume = resume_state
                    .as_deref()
                    .map(|state| match state {
                        "PLAYBACK_START" => Ok(media::ResumeState::PlaybackStart),
                        "PLAYBACK_PAUSE" => Ok(media::ResumeState::PlaybackPause),
                        _ => bail!("resume_state must be PLAYBACK_START or PLAYBACK_PAUSE"),
                    })
                    .transpose()?;
                let (app, id) = self.media_session()?;
                let entry = self.media.seek(app.transport_id, id, time, resume)?;
                self.apply_media(media::Status {
                    request_id: 0,
                    entries: vec![entry],
                })
            }
            NativeCommand::Connect { destination } => {
                self.connection.connect(destination)?;
                Ok(())
            }
            NativeCommand::Disconnect { destination } => {
                self.connection.disconnect(destination)?;
                Ok(())
            }
            NativeCommand::Ping => self.ping(),
            NativeCommand::Pong => {
                self.heartbeat.pong()?;
                Ok(())
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MediaDto {
    content_id: String,
    content_type: String,
    stream_type: String,
    metadata: Option<Value>,
    duration: Option<f64>,
}

/// Native media uses contentId, contentType, streamType, metadata?, duration?.
pub fn parse_media(value: &Value) -> Result<media::Media> {
    let dto: MediaDto =
        serde_json::from_value(value.clone()).context("invalid native media DTO")?;
    ensure!(!dto.content_id.is_empty(), "contentId is empty");
    ensure!(!dto.content_type.is_empty(), "contentType is empty");
    let stream_type = match dto.stream_type.as_str() {
        "NONE" => media::StreamType::None,
        "BUFFERED" => media::StreamType::Buffered,
        "LIVE" => media::StreamType::Live,
        _ => bail!("streamType must be NONE, BUFFERED or LIVE"),
    };
    Ok(media::Media {
        content_id: dto.content_id,
        content_type: dto.content_type,
        stream_type,
        metadata: dto.metadata.as_ref().map(parse_metadata).transpose()?,
        duration: dto
            .duration
            .map(|value| nonnegative_f32(value, "duration"))
            .transpose()?,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueueDto {
    items: Vec<QueueItemDto>,
    start_index: u16,
    queue_type: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueItemDto {
    media: Value,
}

/// Native queue uses items:[{media:...}], startIndex:u16 and queueType.
pub fn parse_queue(value: &Value) -> Result<media::MediaQueue> {
    let dto: QueueDto =
        serde_json::from_value(value.clone()).context("invalid native queue DTO")?;
    ensure!(!dto.items.is_empty(), "queue items must not be empty");
    ensure!(
        (dto.start_index as usize) < dto.items.len(),
        "startIndex is outside queue items"
    );
    let items = dto
        .items
        .into_iter()
        .map(|item| {
            Ok(media::QueueItem {
                media: parse_media(&item.media)?,
            })
        })
        .collect::<Result<_>>()?;
    Ok(media::MediaQueue {
        items,
        start_index: dto.start_index,
        queue_type: media::QueueType::from_str(&dto.queue_type)?,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OptionsDto {
    #[serde(default)]
    current_time: f64,
    #[serde(default = "autoplay_default")]
    autoplay: bool,
}

fn autoplay_default() -> bool {
    true
}

pub fn parse_load_options(value: &Value) -> Result<media::LoadOptions> {
    let dto: OptionsDto =
        serde_json::from_value(value.clone()).context("invalid native load options")?;
    ensure!(
        dto.current_time.is_finite() && dto.current_time >= 0.0,
        "currentTime must be finite and nonnegative"
    );
    Ok(media::LoadOptions {
        current_time: dto.current_time,
        autoplay: dto.autoplay,
    })
}

fn nonnegative_f32(value: f64, field: &str) -> Result<f32> {
    ensure!(
        value.is_finite() && value >= 0.0 && value <= f32::MAX as f64,
        "{field} must be finite, nonnegative and representable as f32"
    );
    Ok(value as f32)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageDto {
    url: String,
    width: Option<u32>,
    height: Option<u32>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetadataDto {
    #[serde(default)]
    metadata_type: u8,
    title: Option<String>,
    subtitle: Option<String>,
    #[serde(default)]
    images: Vec<ImageDto>,
    release_date: Option<String>,
    studio: Option<String>,
    series_title: Option<String>,
    episode_title: Option<String>,
    season: Option<u32>,
    episode: Option<u32>,
    original_air_date: Option<String>,
    album_name: Option<String>,
    album_artist: Option<String>,
    artist: Option<String>,
    composer: Option<String>,
    track_number: Option<u32>,
    disc_number: Option<u32>,
    location: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    width: Option<u32>,
    height: Option<u32>,
    creation_date_time: Option<String>,
}

fn paired<T>(left: Option<T>, right: Option<T>, field: &str) -> Result<Option<(T, T)>> {
    match (left, right) {
        (Some(left), Some(right)) => Ok(Some((left, right))),
        (None, None) => Ok(None),
        _ => bail!("{field} must be supplied together"),
    }
}

pub fn parse_metadata(value: &Value) -> Result<media::Metadata> {
    let dto: MetadataDto = serde_json::from_value(value.clone()).context("invalid metadata DTO")?;
    let fields: &[&str] = match dto.metadata_type {
        0 => &["title", "subtitle", "releaseDate"],
        1 => &["title", "subtitle", "studio", "releaseDate"],
        2 => &[
            "seriesTitle",
            "episodeTitle",
            "season",
            "episode",
            "originalAirDate",
        ],
        3 => &[
            "title",
            "albumName",
            "albumArtist",
            "artist",
            "composer",
            "trackNumber",
            "discNumber",
            "releaseDate",
        ],
        4 => &[
            "title",
            "artist",
            "location",
            "latitude",
            "longitude",
            "width",
            "height",
            "creationDateTime",
        ],
        _ => bail!("metadataType must be 0, 1, 2, 3 or 4"),
    };
    for field in value
        .as_object()
        .ok_or_else(|| anyhow!("metadata must be an object"))?
        .keys()
    {
        ensure!(
            field == "metadataType"
                || (field == "images" && dto.metadata_type != 4)
                || fields.contains(&field.as_str()),
            "metadata field {field} is not supported for metadataType {}",
            dto.metadata_type
        );
    }
    let images = dto
        .images
        .into_iter()
        .map(|image| {
            Ok(media::Image {
                url: image.url,
                dimensions: paired(image.width, image.height, "image width/height")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(match dto.metadata_type {
        0 => media::Metadata::Generic(media::GenericMediaMetadata {
            title: dto.title,
            subtitle: dto.subtitle,
            images,
            release_date: dto.release_date,
        }),
        1 => media::Metadata::Movie(media::MovieMediaMetadata {
            title: dto.title,
            subtitle: dto.subtitle,
            studio: dto.studio,
            images,
            release_date: dto.release_date,
        }),
        2 => media::Metadata::TvShow(media::TvShowMediaMetadata {
            series_title: dto.series_title,
            episode_title: dto.episode_title,
            season: dto.season,
            episode: dto.episode,
            images,
            original_air_date: dto.original_air_date,
        }),
        3 => media::Metadata::MusicTrack(media::MusicTrackMediaMetadata {
            album_name: dto.album_name,
            title: dto.title,
            album_artist: dto.album_artist,
            artist: dto.artist,
            composer: dto.composer,
            track_number: dto.track_number,
            disc_number: dto.disc_number,
            images,
            release_date: dto.release_date,
        }),
        4 => media::Metadata::Photo(media::PhotoMediaMetadata {
            title: dto.title,
            artist: dto.artist,
            location: dto.location,
            latitude_longitude: paired(dto.latitude, dto.longitude, "latitude/longitude")?,
            dimensions: paired(dto.width, dto.height, "photo width/height")?,
            creation_date_time: dto.creation_date_time,
        }),
        _ => unreachable!(),
    })
}

fn legacy_load(load: MediaLoad) -> Result<(media::Media, media::LoadOptions)> {
    ensure!(
        !load.enqueue && load.subtitles.is_none(),
        "enqueue and subtitle tracks are not supported by rust_cast"
    );
    ensure!(
        load.subtitles_lang == "en-US"
            && load.subtitles_mime == "text/vtt"
            && load.subtitle_id == 1,
        "non-default subtitle options are not supported by rust_cast"
    );
    let mut metadata = load.metadata.unwrap_or_else(|| json!({}));
    ensure!(metadata.is_object(), "metadata must be an object");
    if let Some(title) = load.title {
        let field = if metadata.get("metadataType").and_then(Value::as_u64) == Some(2) {
            "episodeTitle"
        } else {
            "title"
        };
        metadata[field] = json!(title);
    }
    if let Some(thumb) = load.thumb {
        if metadata.get("images").is_none() {
            metadata["images"] = json!([]);
        }
        metadata["images"]
            .as_array_mut()
            .ok_or_else(|| anyhow!("metadata images must be an array"))?
            .push(json!({"url":thumb}));
    }
    let metadata = if metadata.as_object().unwrap().is_empty() {
        None
    } else {
        Some(metadata)
    };
    let media = parse_media(
        &json!({"contentId":load.url,"contentType":load.content_type.unwrap_or_else(|| "video/mp4".into()),
        "streamType":load.stream_type,"metadata":metadata}),
    )?;
    let options = parse_load_options(
        &json!({"currentTime":load.current_time.unwrap_or(0.0),"autoplay":load.autoplay}),
    )?;
    Ok((media, options))
}

pub fn receiver_status_payload(status: &receiver::Status) -> Value {
    let applications:Vec<_>=status.applications.iter().map(|app| json!({
        "appId":app.app_id,"sessionId":app.session_id,"transportId":app.transport_id,
        "namespaces":app.namespaces.iter().map(|name| json!({"name":name})).collect::<Vec<_>>(),
        "displayName":app.display_name,"statusText":app.status_text,
    })).collect();
    let mut volume = json!({});
    if let Some(level) = status.volume.level {
        volume["level"] = json!(level);
    }
    if let Some(muted) = status.volume.muted {
        volume["muted"] = json!(muted);
    }
    json!({"type":"RECEIVER_STATUS","requestId":status.request_id,"status":{
        "applications":applications,"isActiveInput":status.is_active_input,"isStandBy":status.is_stand_by,"volume":volume,
    }})
}

pub fn media_status_payload(status: &media::Status) -> Value {
    let entries:Vec<_>=status.entries.iter().map(|entry| {
        let mut value=json!({"mediaSessionId":entry.media_session_id,"playbackRate":entry.playback_rate,
            "playerState":entry.player_state.to_string(),"supportedMediaCommands":entry.supported_media_commands});
        if let Some(media)=&entry.media {value["media"]=media_value(media);}
        if let Some(time)=entry.current_time {value["currentTime"]=json!(time);}
        if let Some(id)=entry.current_item_id {value["currentItemId"]=json!(id);}
        if let Some(id)=entry.loading_item_id {value["loadingItemId"]=json!(id);}
        if let Some(id)=entry.preloaded_item_id {value["preloadedItemId"]=json!(id);}
        if let Some(reason)=entry.idle_reason {value["idleReason"]=json!(match reason {
            media::IdleReason::Cancelled=>"CANCELLED",media::IdleReason::Interrupted=>"INTERRUPTED",
            media::IdleReason::Finished=>"FINISHED",media::IdleReason::Error=>"ERROR",
        });}
        if let Some(extended)=&entry.extended_status {
            let mut data=json!({"playerState":extended.player_state.to_string()});
            if let Some(id)=extended.media_session_id {data["mediaSessionId"]=json!(id);}
            if let Some(media)=&extended.media {data["media"]=media_value(media);}
            value["extendedStatus"]=data;
        }
        value
    }).collect();
    json!({"type":"MEDIA_STATUS","requestId":status.request_id,"status":entries})
}

fn media_value(media: &media::Media) -> Value {
    let mut value = json!({"contentId":media.content_id,"contentType":media.content_type,"streamType":media.stream_type.to_string()});
    if let Some(duration) = media.duration {
        value["duration"] = json!(duration);
    }
    if let Some(metadata) = &media.metadata {
        value["metadata"] = metadata_value(metadata);
    }
    value
}

fn image_values(images: &[media::Image]) -> Vec<Value> {
    images
        .iter()
        .map(|image| {
            let mut value = json!({"url":image.url});
            if let Some((width, height)) = image.dimensions {
                value["width"] = json!(width);
                value["height"] = json!(height);
            }
            value
        })
        .collect()
}

fn metadata_value(metadata: &media::Metadata) -> Value {
    let mut value = match metadata {
        media::Metadata::Generic(data) => {
            json!({"metadataType":0,"title":data.title,"subtitle":data.subtitle,"images":image_values(&data.images),"releaseDate":data.release_date})
        }
        media::Metadata::Movie(data) => {
            json!({"metadataType":1,"title":data.title,"subtitle":data.subtitle,"studio":data.studio,"images":image_values(&data.images),"releaseDate":data.release_date})
        }
        media::Metadata::TvShow(data) => {
            json!({"metadataType":2,"seriesTitle":data.series_title,"episodeTitle":data.episode_title,"season":data.season,"episode":data.episode,"images":image_values(&data.images),"originalAirDate":data.original_air_date})
        }
        media::Metadata::MusicTrack(data) => {
            json!({"metadataType":3,"title":data.title,"albumName":data.album_name,"albumArtist":data.album_artist,"artist":data.artist,"composer":data.composer,"trackNumber":data.track_number,"discNumber":data.disc_number,"images":image_values(&data.images),"releaseDate":data.release_date})
        }
        media::Metadata::Photo(data) => {
            let mut value = json!({"metadataType":4,"title":data.title,"artist":data.artist,"location":data.location,"creationDateTime":data.creation_date_time});
            if let Some((latitude, longitude)) = data.latitude_longitude {
                value["latitude"] = json!(latitude);
                value["longitude"] = json!(longitude);
            }
            if let Some((width, height)) = data.dimensions {
                value["width"] = json!(width);
                value["height"] = json!(height);
            }
            value
        }
    };
    value
        .as_object_mut()
        .unwrap()
        .retain(|_, value| !value.is_null());
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn media_dto() -> Value {
        json!({"contentId":"https://example.test/media.mp4","contentType":"video/mp4","streamType":"BUFFERED"})
    }

    #[test]
    fn native_media_options_and_queue_validation() {
        assert_eq!(
            parse_media(&media_dto()).unwrap().stream_type,
            media::StreamType::Buffered
        );
        let mut bad = media_dto();
        bad["tracks"] = json!([]);
        assert!(parse_media(&bad).is_err());
        let mut bad = media_dto();
        bad["streamType"] = json!("unknown");
        assert!(parse_media(&bad).is_err());
        let mut bad = media_dto();
        bad["duration"] = json!(1e100);
        assert!(parse_media(&bad).is_err());
        let mut bad = media_dto();
        bad["duration"] = json!(-1);
        assert!(parse_media(&bad).is_err());
        let queue = json!({"items":[{"media":media_dto()}],"startIndex":0,"queueType":"PLAYLIST"});
        assert_eq!(parse_queue(&queue).unwrap().items.len(), 1);
        let mut bad = queue.clone();
        bad["repeatMode"] = json!("REPEAT_ALL");
        assert!(parse_queue(&bad).is_err());
        let mut bad = queue.clone();
        bad["startIndex"] = json!(1);
        assert!(parse_queue(&bad).is_err());
        let mut bad = queue;
        bad["items"][0]["autoplay"] = json!(true);
        assert!(parse_queue(&bad).is_err());
        assert!(parse_load_options(&json!({"tracks":[]})).is_err());
        assert!(parse_load_options(&json!({"currentTime":-1})).is_err());
        let options = parse_load_options(&json!({"currentTime":12.5,"autoplay":false})).unwrap();
        assert_eq!(options.current_time, 12.5);
        assert!(!options.autoplay);
    }

    #[test]
    fn metadata_all_native_types_roundtrip_without_unknown_fields() {
        let examples = [
            json!({"metadataType":0,"title":"Generic","subtitle":"Description","releaseDate":"2024-01-01","images":[{"url":"art","width":64,"height":32}]}),
            json!({"metadataType":1,"title":"Movie","subtitle":"Description","studio":"Studio","releaseDate":"2024-01-01","images":[{"url":"art"}]}),
            json!({"metadataType":2,"seriesTitle":"Series","episodeTitle":"Episode","season":1,"episode":2,"originalAirDate":"2024-01-01","images":[{"url":"art"}]}),
            json!({"metadataType":3,"title":"Track","artist":"Artist","albumName":"Album","albumArtist":"Album Artist","composer":"Composer","trackNumber":4,"discNumber":2,"releaseDate":"2024-01-01","images":[{"url":"art"}]}),
            json!({"metadataType":4,"title":"Photo","artist":"Artist","location":"Location","latitude":12.0,"longitude":13.0,"width":100,"height":200,"creationDateTime":"2024-01-01T00:00:00Z"}),
        ];
        for value in examples {
            let metadata = parse_metadata(&value).unwrap();
            assert_eq!(
                parse_metadata(&metadata_value(&metadata)).unwrap(),
                metadata
            );
        }
        assert!(parse_metadata(&json!({"metadataType":5})).is_err());
        assert!(parse_metadata(&json!({"metadataType":0,"artist":"unsupported here"})).is_err());
        assert!(parse_metadata(&json!({"metadataType":4,"latitude":1})).is_err());
        assert!(parse_metadata(&json!({"images":[{"url":"art","width":10}]})).is_err());
        assert!(parse_metadata(&json!({"unexpected":true})).is_err());
        for images in [json!([]), json!([{"url":"art"}]), Value::Null] {
            assert!(parse_metadata(&json!({"metadataType":4,"images":images})).is_err());
        }
        let photo = parse_metadata(&json!({"metadataType":4,"title":"Photo"})).unwrap();
        assert!(metadata_value(&photo).get("images").is_none());
        let mut nested = media_dto();
        nested["metadata"] = json!({"metadataType":4,"images":[]});
        assert!(parse_media(&nested).is_err());
        assert!(parse_queue(
            &json!({"items":[{"media":nested}],"startIndex":0,"queueType":"PLAYLIST"})
        )
        .is_err());
    }

    #[test]
    fn typed_media_status_omits_missing_fields_for_cache_merge() {
        let entry = media::StatusEntry {
            media_session_id: 4,
            media: None,
            playback_rate: 1.0,
            player_state: media::PlayerState::Paused,
            current_item_id: None,
            loading_item_id: None,
            preloaded_item_id: None,
            idle_reason: None,
            extended_status: None,
            current_time: None,
            supported_media_commands: 3,
        };
        let value = media_status_payload(&media::Status {
            request_id: 1,
            entries: vec![entry],
        });
        assert!(value["status"][0].get("media").is_none());
        assert!(value["status"][0].get("currentTime").is_none());
        assert_eq!(value["status"][0]["playerState"], "PAUSED");
        let mut cache = crate::status::StatusCache::new();
        cache.apply(MEDIA,&json!({"status":[{"media":{"contentId":"movie","duration":100.0,"metadata":{"title":"Title"}},"currentTime":42.0,"playerState":"PLAYING"}]}));
        let updates = cache.apply(MEDIA, &value);
        assert!(!updates.iter().any(|(key, _)| key == "media/title"
            || key == "player_duration"
            || key == "player_position"));
    }

    #[test]
    fn legacy_load_uses_only_native_media_fields() {
        let load: MediaLoad = serde_json::from_value(json!({
            "url":"https://example.test/movie.mp4", "content_type":null,
            "title":"Movie", "thumb":"https://example.test/art.jpg",
            "current_time":12.0, "autoplay":false,
        }))
        .unwrap();
        let (media, options) = legacy_load(load).unwrap();
        assert_eq!(media.content_type, "video/mp4");
        let metadata = metadata_value(media.metadata.as_ref().unwrap());
        assert_eq!(metadata["title"], "Movie");
        assert_eq!(metadata["images"][0]["url"], "https://example.test/art.jpg");
        assert!(metadata.get("thumb").is_none());
        assert_eq!(options.current_time, 12.0);
        assert!(!options.autoplay);
    }

    #[test]
    fn legacy_photo_thumbnail_and_unrepresentable_metadata_are_rejected() {
        let mut load: MediaLoad = serde_json::from_value(json!({
            "url":"https://example.test/photo.jpg","content_type":"image/jpeg",
            "metadata":{"metadataType":4,"title":"Photo"},"thumb":"art",
        }))
        .unwrap();
        assert!(legacy_load(load.clone()).is_err());
        load.thumb = None;
        assert!(legacy_load(load.clone()).is_ok());
        load.subtitles_lang = "de-DE".into();
        assert!(legacy_load(load).is_err());
        let load: MediaLoad = serde_json::from_value(json!({
            "url":"https://example.test/episode.mp4","content_type":"video/mp4",
            "metadata":{"metadataType":2,"seriesTitle":"Series"},"title":"Episode",
        }))
        .unwrap();
        let (media, _) = legacy_load(load).unwrap();
        assert_eq!(
            metadata_value(media.metadata.as_ref().unwrap())["episodeTitle"],
            "Episode"
        );
    }

    #[test]
    fn typed_media_status_preserves_extended_status_and_metadata() {
        let media = parse_media(&json!({
            "contentId":"movie", "contentType":"video/mp4", "streamType":"BUFFERED",
            "duration":120.0, "metadata":{"metadataType":1,"title":"Movie"},
        }))
        .unwrap();
        let status = media::Status {
            request_id: 7,
            entries: vec![media::StatusEntry {
                media_session_id: 9,
                media: None,
                playback_rate: 1.0,
                player_state: media::PlayerState::Idle,
                current_item_id: Some(2),
                loading_item_id: Some(3),
                preloaded_item_id: Some(4),
                idle_reason: Some(media::IdleReason::Interrupted),
                extended_status: Some(media::ExtendedStatus {
                    player_state: media::ExtendedPlayerState::Loading,
                    media_session_id: Some(10),
                    media: Some(media),
                }),
                current_time: Some(0.0),
                supported_media_commands: 3,
            }],
        };
        let value = media_status_payload(&status);
        assert_eq!(value["requestId"], 7);
        assert_eq!(value["status"][0]["idleReason"], "INTERRUPTED");
        assert_eq!(
            value["status"][0]["extendedStatus"]["playerState"],
            "LOADING"
        );
        assert_eq!(
            value["status"][0]["extendedStatus"]["media"]["metadata"]["title"],
            "Movie"
        );
        let mut cache = crate::status::StatusCache::new();
        assert!(cache
            .apply(MEDIA, &value)
            .iter()
            .any(|(key, value)| key == "media/title" && value == "Movie"));
    }

    #[test]
    fn receiver_status_maps_apps_and_optional_volume() {
        let status = receiver::Status {
            request_id: 3,
            applications: vec![receiver::Application {
                app_id: "id".into(),
                session_id: "session".into(),
                transport_id: "web-1".into(),
                namespaces: vec![MEDIA.into()],
                display_name: "Player".into(),
                status_text: "Ready".into(),
            }],
            is_active_input: true,
            is_stand_by: false,
            volume: receiver::Volume {
                level: Some(0.5),
                muted: None,
            },
        };
        let value = receiver_status_payload(&status);
        assert_eq!(
            value["status"]["applications"][0]["namespaces"][0]["name"],
            MEDIA
        );
        assert_eq!(value["status"]["volume"]["level"], 0.5);
        assert!(value["status"]["volume"].get("muted").is_none());
    }

    struct Fixture {
        budget: Rc<Budget>,
        _commands: mpsc::Sender<Command>,
        _events: mpsc::Receiver<Event>,
        shutdown: watch::Sender<bool>,
    }

    impl Fixture {
        fn new() -> Self {
            let (commands, receiver) = mpsc::channel(4);
            let (events, event_receiver) = mpsc::channel(32);
            let (shutdown, watch) = watch::channel(false);
            let control = Control {
                commands: Rc::new(RefCell::new(receiver)),
                shutdown: watch,
                events,
                name: "test".into(),
            };
            let budget = Rc::new(Budget {
                deadline: Cell::new(Instant::now()),
                bytes_read: Cell::new(0),
                phase: Cell::new(BudgetPhase::Operation),
                control,
            });
            budget.begin(OPERATION_TIMEOUT);
            Self {
                budget,
                _commands: commands,
                _events: event_receiver,
                shutdown,
            }
        }
    }

    struct ScriptedStream {
        input: io::Cursor<Vec<u8>>,
        output: Rc<RefCell<Vec<u8>>>,
    }

    impl Read for ScriptedStream {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.input.position() as usize >= self.input.get_ref().len() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "scripted timeout"));
            }
            self.input.read(buffer)
        }
    }

    impl Write for ScriptedStream {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.output.borrow_mut().extend_from_slice(buffer);
            Ok(buffer.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn application(session_id: &str, transport_id: &str) -> receiver::Application {
        receiver::Application {
            app_id: "app".into(),
            session_id: session_id.into(),
            transport_id: transport_id.into(),
            namespaces: vec![MEDIA.into()],
            display_name: "Player".into(),
            status_text: "Ready".into(),
        }
    }

    fn receiver_status(applications: Vec<receiver::Application>) -> receiver::Status {
        receiver::Status {
            request_id: 0,
            applications,
            is_active_input: true,
            is_stand_by: false,
            volume: receiver::Volume {
                level: Some(0.5),
                muted: Some(false),
            },
        }
    }

    fn scripted_session(
        fixture: &Fixture,
        replies: &[(&str, Value)],
    ) -> (Session<ScriptedStream>, Rc<RefCell<Vec<u8>>>) {
        use rust_cast::message_manager::{CastMessage, CastMessagePayload};
        let encoded = Rc::new(RefCell::new(Vec::new()));
        let writer = MessageManager::new(ScriptedStream {
            input: io::Cursor::new(Vec::new()),
            output: encoded.clone(),
        });
        for (namespace, payload) in replies {
            writer
                .send(CastMessage {
                    namespace: (*namespace).into(),
                    source: if *namespace == RECEIVER {
                        PLATFORM.into()
                    } else {
                        "web-1".into()
                    },
                    destination: SENDER.into(),
                    payload: CastMessagePayload::String(payload.to_string()),
                })
                .unwrap();
        }
        let output = Rc::new(RefCell::new(Vec::new()));
        let manager = Rc::new(MessageManager::new(TrackedStream {
            stream: ScriptedStream {
                input: io::Cursor::new(encoded.borrow().clone()),
                output: output.clone(),
            },
            budget: fixture.budget.clone(),
        }));
        (
            Session {
                control: fixture.budget.control.clone(),
                budget: fixture.budget.clone(),
                manager: manager.clone(),
                connection: ConnectionChannel::new(SENDER, manager.clone()),
                heartbeat: HeartbeatChannel::new(SENDER, PLATFORM, manager.clone()),
                receiver: ReceiverChannel::new(SENDER, PLATFORM, manager.clone()),
                media: MediaChannel::new(SENDER, manager),
                application: None,
                receiver_status: None,
                media_id: None,
                pending_ping: None,
            },
            output,
        )
    }

    fn sent_messages(output: &Rc<RefCell<Vec<u8>>>) -> Vec<(String, Value)> {
        use rust_cast::message_manager::CastMessagePayload;
        let reader = MessageManager::new(ScriptedStream {
            input: io::Cursor::new(output.borrow().clone()),
            output: Rc::new(RefCell::new(Vec::new())),
        });
        let mut messages = Vec::new();
        while let Ok(message) = reader.receive() {
            let CastMessagePayload::String(payload) = message.payload else {
                panic!("expected JSON");
            };
            messages.push((message.destination, serde_json::from_str(&payload).unwrap()));
        }
        messages
    }

    #[test]
    fn initial_status_attaches_existing_app_without_launch_or_media_commands() {
        let fixture = Fixture::new();
        let mut status =
            receiver_status_payload(&receiver_status(vec![application("session", "web-1")]));
        status["requestId"] = json!(1);
        let (mut session, output) = scripted_session(
            &fixture,
            &[
                (RECEIVER, status),
                (
                    MEDIA,
                    json!({"type":"MEDIA_STATUS","requestId":2,"status":[{
                        "mediaSessionId":42,"playbackRate":1.0,"playerState":"PLAYING","supportedMediaCommands":3
                    }]}),
                ),
            ],
        );
        session.poll().unwrap();
        assert_eq!(session.application.as_ref().unwrap().transport_id, "web-1");
        assert_eq!(session.media_id, Some(42));
        let messages = sent_messages(&output);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].1["type"], "GET_STATUS");
        assert_eq!(messages[1].1["type"], "CONNECT");
        assert_eq!(messages[1].0, "web-1");
        assert_eq!(messages[2].1["type"], "GET_STATUS");
        assert!(messages
            .iter()
            .all(|(_, payload)| payload["type"] != "LAUNCH"
                && payload["type"] != "LOAD"
                && payload["type"] != "STOP"));
    }

    #[test]
    fn stop_app_forgets_cached_session_even_if_refresh_fails() {
        let fixture = Fixture::new();
        let mut status = receiver_status_payload(&receiver_status(Vec::new()));
        status["requestId"] = json!(1);
        let (mut session, output) = scripted_session(&fixture, &[(RECEIVER, status)]);
        session.application = Some(application("session", "web-1"));
        session.receiver_status = Some(receiver_status(vec![application("session", "web-1")]));
        session.media_id = Some(42);
        let error = session
            .native(NativeCommand::StopApp { session_id: None })
            .unwrap_err();
        assert!(fatal(&error)); // only the follow-up GET_STATUS timed out
        assert!(session.application.is_none());
        assert!(session.media_id.is_none());
        assert!(session
            .receiver_status
            .as_ref()
            .unwrap()
            .applications
            .is_empty());
        let messages = sent_messages(&output);
        assert_eq!(messages[0].1["type"], "STOP");
        assert_eq!(messages[0].1["sessionId"], "session");
        assert_eq!(messages[1].1["type"], "GET_STATUS");
    }

    #[test]
    fn stopping_another_app_does_not_retire_the_active_media_session() {
        let fixture = Fixture::new();
        let (mut session, _) = scripted_session(&fixture, &[]);
        session.application = Some(application("current", "web-1"));
        session.receiver_status = Some(receiver_status(vec![
            application("current", "web-1"),
            application("other", "web-2"),
        ]));
        session.media_id = Some(42);
        session.forget_stopped_app("other").unwrap();
        assert_eq!(session.media_id, Some(42));
        assert_eq!(session.application.as_ref().unwrap().session_id, "current");
        assert_eq!(
            session.receiver_status.as_ref().unwrap().applications.len(),
            1
        );
    }

    #[test]
    fn no_app_and_changed_app_clear_old_media_but_failed_attachment_does_not_launch() {
        let mut fixture = Fixture::new();
        let (mut session, output) = scripted_session(
            &fixture,
            &[(
                MEDIA,
                json!({"type":"INVALID_REQUEST","requestId":1,"reason":"no active media"}),
            )],
        );
        session.attach(None).unwrap();
        assert!(
            matches!(fixture._events.try_recv().unwrap(),Event::Update {payload,..} if payload["status"] == json!([]))
        );
        let error = session
            .apply_receiver(receiver_status(vec![application("new", "web-2")]))
            .unwrap_err();
        assert!(!fatal(&error));
        assert_eq!(session.application.as_ref().unwrap().session_id, "new");
        assert_eq!(session.media_id, None);
        assert!(sent_messages(&output)
            .iter()
            .all(|(_, payload)| payload["type"] != "LAUNCH"));
    }

    #[test]
    fn buffered_pong_is_drained_before_checking_an_expired_heartbeat() {
        let fixture = Fixture::new();
        let mut status = receiver_status_payload(&receiver_status(Vec::new()));
        status["requestId"] = json!(1);
        let (mut session, _) = scripted_session(
            &fixture,
            &[
                (
                    "urn:x-cast:com.google.cast.tp.heartbeat",
                    json!({"type":"PONG"}),
                ),
                (RECEIVER, status),
            ],
        );
        session.pending_ping = Some(Instant::now() - HEARTBEAT_TIMEOUT - Duration::from_secs(1));
        session.receiver.get_status().unwrap();
        session.budget.begin(IDLE_READ);
        let pong = session.manager.receive().unwrap();
        assert_eq!(session.budget.bytes_read.get(), 0);
        assert!(matches!(
            session.heartbeat.parse(&pong).unwrap(),
            HeartbeatResponse::Pong
        ));
        session.pending_ping = None;
        session.check_heartbeat().unwrap();
    }

    #[test]
    fn a_new_media_session_clears_old_metadata_but_same_session_deltas_do_not() {
        let mut fixture = Fixture::new();
        let (mut session, _) = scripted_session(&fixture, &[]);
        session.media_id = Some(42);
        let mut cache = crate::status::StatusCache::new();
        cache.apply(
            MEDIA,
            &json!({"status":[{
                "mediaSessionId":42,"playerState":"PLAYING","media":{"metadata":{"title":"Old"}}
            }]}),
        );
        let entry = media::StatusEntry {
            media_session_id: 42,
            media: None,
            playback_rate: 1.0,
            player_state: media::PlayerState::Paused,
            current_item_id: None,
            loading_item_id: None,
            preloaded_item_id: None,
            idle_reason: None,
            extended_status: None,
            current_time: None,
            supported_media_commands: 3,
        };
        session
            .apply_media(media::Status {
                request_id: 0,
                entries: vec![entry.clone()],
            })
            .unwrap();
        let Event::Update {
            namespace, payload, ..
        } = fixture._events.try_recv().unwrap()
        else {
            panic!("expected media update");
        };
        cache.apply(&namespace, &payload);
        assert!(cache
            .snapshot()
            .iter()
            .any(|(key, value)| key == "media/title" && value == "Old"));
        let entry = media::StatusEntry {
            media_session_id: 43,
            ..entry
        };
        session
            .apply_media(media::Status {
                request_id: 0,
                entries: vec![entry],
            })
            .unwrap();
        while let Ok(Event::Update {
            namespace, payload, ..
        }) = fixture._events.try_recv()
        {
            cache.apply(&namespace, &payload);
        }
        assert!(cache
            .snapshot()
            .iter()
            .any(|(key, value)| key == "media/title" && value.is_empty()));
    }

    #[test]
    fn tracked_reads_distinguish_idle_and_partial_upstream_frames() {
        use rust_cast::message_manager::{CastMessage, CastMessagePayload};
        let output = Rc::new(RefCell::new(Vec::new()));
        let writer = MessageManager::new(ScriptedStream {
            input: io::Cursor::new(Vec::new()),
            output: output.clone(),
        });
        writer
            .send(CastMessage {
                namespace: MEDIA.into(),
                source: "web-1".into(),
                destination: SENDER.into(),
                payload: CastMessagePayload::String(
                    json!({"type":"MEDIA_STATUS", "status":[]}).to_string(),
                ),
            })
            .unwrap();
        let frame = output.borrow().clone();
        for end in [0, 2, frame.len() - 1, frame.len()] {
            let fixture = Fixture::new();
            let stream = TrackedStream {
                stream: ScriptedStream {
                    input: io::Cursor::new(frame[..end].to_vec()),
                    output: Rc::new(RefCell::new(Vec::new())),
                },
                budget: fixture.budget.clone(),
            };
            let reader = MessageManager::new(stream);
            let result = reader.receive();
            assert_eq!(fixture.budget.bytes_read.get(), end);
            if end == frame.len() {
                assert_eq!(result.unwrap().namespace, MEDIA);
                fixture.budget.begin(IDLE_READ);
                assert!(idle_timeout(
                    &reader.receive().unwrap_err(),
                    fixture.budget.bytes_read.get()
                ));
            } else {
                assert_eq!(
                    idle_timeout(&result.unwrap_err(), fixture.budget.bytes_read.get()),
                    end == 0
                );
            }
        }
    }

    #[test]
    fn deadline_applies_even_when_plaintext_is_buffered_and_shutdown_is_not_interrupted() {
        let fixture = Fixture::new();
        let mut stream = TrackedStream {
            stream: io::Cursor::new(vec![1, 2, 3]),
            budget: fixture.budget.clone(),
        };
        fixture.budget.deadline.set(Instant::now());
        assert_eq!(
            stream.read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(fixture.budget.bytes_read.get(), 0);
        fixture.budget.begin(OPERATION_TIMEOUT);
        fixture.shutdown.send(true).unwrap();
        assert_eq!(
            stream.read_exact(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
    }

    #[test]
    fn socket_read_obeys_absolute_deadline() {
        let fixture = Fixture::new();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_peer, _) = listener.accept().unwrap();
        let mut socket = TimedSocket {
            socket,
            budget: fixture.budget.clone(),
        };
        fixture.budget.begin(Duration::from_millis(30));
        let started = Instant::now();
        assert!(is_timeout(&socket.read(&mut [0; 1]).unwrap_err()));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn a_fragmented_frame_starting_at_the_idle_deadline_gets_a_full_receive_budget() {
        use rust_cast::message_manager::{CastMessage, CastMessagePayload};

        struct LateFirstFragment {
            input: io::Cursor<Vec<u8>>,
            budget: Rc<Budget>,
            first: bool,
        }

        impl Read for LateFirstFragment {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let limit = buffer.len().min(2);
                let count = self.input.read(&mut buffer[..limit])?;
                if self.first && count > 0 {
                    self.first = false;
                    // Deterministically model arrival at the end of the idle
                    // poll, without depending on OS scheduling or sleeps.
                    self.budget.deadline.set(Instant::now());
                }
                Ok(count)
            }
        }

        impl Write for LateFirstFragment {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                Ok(buffer.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let encoded = Rc::new(RefCell::new(Vec::new()));
        let writer = MessageManager::new(ScriptedStream {
            input: io::Cursor::new(Vec::new()),
            output: encoded.clone(),
        });
        writer
            .send(CastMessage {
                namespace: RECEIVER.into(),
                source: PLATFORM.into(),
                destination: SENDER.into(),
                payload: CastMessagePayload::String(
                    json!({"type":"RECEIVER_STATUS", "status":{}}).to_string(),
                ),
            })
            .unwrap();
        let bytes = encoded.borrow().clone();
        let expected_length = bytes.len();
        let fixture = Fixture::new();
        fixture.budget.begin_receive();
        let reader = MessageManager::new(TrackedStream {
            stream: LateFirstFragment {
                input: io::Cursor::new(bytes),
                budget: fixture.budget.clone(),
                first: true,
            },
            budget: fixture.budget.clone(),
        });
        assert_eq!(reader.receive().unwrap().namespace, RECEIVER);
        assert_eq!(fixture.budget.bytes_read.get(), expected_length);
        assert!(matches!(fixture.budget.phase.get(), BudgetPhase::Frame));
        assert!(fixture.budget.remaining().unwrap() > IDLE_READ);
    }

    #[test]
    fn frame_progress_does_not_extend_an_existing_operation_or_frame_deadline() {
        let fixture = Fixture::new();
        let mut stream = TrackedStream {
            stream: io::Cursor::new(vec![1, 2, 3]),
            budget: fixture.budget.clone(),
        };
        fixture.budget.begin(OPERATION_TIMEOUT);
        let operation_deadline = fixture.budget.deadline.get();
        stream.read_exact(&mut [0; 1]).unwrap();
        assert_eq!(fixture.budget.deadline.get(), operation_deadline);
        assert!(matches!(fixture.budget.phase.get(), BudgetPhase::Operation));

        fixture.budget.begin_receive();
        stream.read_exact(&mut [0; 1]).unwrap();
        assert!(matches!(fixture.budget.phase.get(), BudgetPhase::Frame));
        fixture.budget.deadline.set(Instant::now());
        let error = stream.read(&mut [0; 1]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(error.to_string().contains("frame receive"));
        assert!(error.to_string().contains("1 plaintext bytes"));
        assert_eq!(fixture.budget.bytes_read.get(), 1);
    }

    #[test]
    fn ordinary_status_poll_tolerates_unrelated_timestamp_sized_reply_ids() {
        let fixture = Fixture::new();
        let mut unsolicited = receiver_status_payload(&receiver_status(Vec::new()));
        unsolicited["requestId"] = json!(1792288955061_u64);
        let mut response = receiver_status_payload(&receiver_status(Vec::new()));
        response["requestId"] = json!(1);
        let (mut session, _) =
            scripted_session(&fixture, &[(RECEIVER, unsolicited), (RECEIVER, response)]);
        session.poll().unwrap();
        assert_eq!(session.receiver_status.as_ref().unwrap().request_id, 1);
        let buffered = session.manager.receive().unwrap();
        let ReceiverResponse::Status(status) = session.receiver.parse(&buffered).unwrap() else {
            panic!("expected buffered receiver status");
        };
        assert_eq!(status.request_id, 1792288955061_u64);
    }

    #[test]
    fn idle_timeouts_are_safe_only_without_cast_frame_bytes() {
        let error = CastError::Io(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
        assert!(idle_timeout(&error, 0));
        assert!(!idle_timeout(&error, 1));
        assert!(!idle_timeout(
            &CastError::Io(io::Error::from(io::ErrorKind::UnexpectedEof)),
            0
        ));
        assert!(fatal(&anyhow!(error)));
        assert!(!fatal(&anyhow!(CastError::Internal(
            "invalid media command".into()
        ))));
    }
}
