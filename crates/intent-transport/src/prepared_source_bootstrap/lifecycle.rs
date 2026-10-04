//! Isolated source lifecycle dispatcher. No Store access, generic router or String queue.
use super::*;
use intent_core::note_source_session::{wire, SessionError};
use intent_services::source_session::{Delivery, Open, SourceConnection, SourceWriter};
use std::pin::Pin;
type Socket = WebSocketStream<tokio_rustls::server::TlsStream<CountedTcp>>;
struct Writer<'a>(&'a mut Socket);
impl SourceWriter for Writer<'_> {
    fn start_send(&mut self, frame: String) -> std::result::Result<(), ()> {
        self.0
            .start_send_unpin(Message::Text(frame.into()))
            .map_err(|_| ())
    }
}
// Test-only raw IO observations cover the exclusively owned authorization interval,
// including a cancelled continuation's drain. They are not peer-delivery evidence.
#[cfg(test)]
struct AuthIo<'a> {
    shared: &'a Shared,
    meter: Arc<io::Meter>,
    read: usize,
    written: usize,
}
#[cfg(test)]
impl<'a> AuthIo<'a> {
    fn new(shared: &'a Shared, socket: &Socket) -> Self {
        let meter = socket.get_ref().get_ref().0.meter.clone();
        Self {
            shared,
            read: meter.read.load(Ordering::Acquire),
            written: meter.written.load(Ordering::Acquire),
            meter,
        }
    }
}
#[cfg(test)]
impl Drop for AuthIo<'_> {
    fn drop(&mut self) {
        self.shared.auth_io.lock().unwrap().push((
            self.read,
            self.meter.read.load(Ordering::Acquire),
            self.written,
            self.meter.written.load(Ordering::Acquire),
        ));
    }
}
fn failure(error: SessionError) -> Error {
    if error == SessionError::Uncertain {
        Error::Internal(error.code().into())
    } else {
        Error::InvalidParams(error.code().into())
    }
}
async fn expired(expiry: Option<i128>) {
    let Some(expiry) = expiry else {
        std::future::pending::<()>().await;
        return;
    };
    let remaining = expiry.saturating_sub(time::OffsetDateTime::now_utc().unix_timestamp_nanos());
    if remaining <= 0 {
        return;
    }
    let seconds = u64::try_from(remaining / 1_000_000_000).unwrap_or(u64::MAX);
    let nanos = u32::try_from(remaining % 1_000_000_000).unwrap_or(0);
    tokio::time::sleep(Duration::new(seconds, nanos)).await;
}
/// A completed incoming message/error while work is pending is terminal, not a
/// transparent read retry. Pending partial input stays in the same owned parser
/// if work wins the select; it is neither a second dispatched request nor proof
/// of inactivity. The original work continuation is always drained below.
async fn work<F: Future>(
    future: Pin<&mut F>,
    ws: &mut Option<Socket>,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
    expiry: Option<i128>,
    source: Option<&SourceConnection>,
    shared: &Shared,
) -> Option<F::Output> {
    if *stop.borrow() || revoke.now() {
        return None;
    }
    #[cfg(test)]
    let input = futures_util::future::poll_fn(|cx| {
        let socket = ws.as_mut().expect("owned source socket");
        let before = socket
            .get_ref()
            .get_ref()
            .0
            .meter
            .read
            .load(Ordering::Acquire);
        let polled = socket.poll_next_unpin(cx);
        let after = socket
            .get_ref()
            .get_ref()
            .0
            .meter
            .read
            .load(Ordering::Acquire);
        if polled.is_pending() && after > before {
            shared.source_partial.fetch_add(1, Ordering::AcqRel);
        }
        polled
    });
    #[cfg(not(test))]
    let input = {
        let _ = shared;
        ws.as_mut().expect("owned source socket").next()
    };
    tokio::select! {biased;
        ()=cancelled(stop)=>None,
        ()=revoke.wait()=>None,
        ()=expired(expiry)=>None,
        ()=async { if let Some(source) = source { source.revoked().await } else { std::future::pending().await } }=>None,
        _=input=>None,
        result=future=>Some(result),
    }
}
/// A ready writer stays exclusively owned through final authorization. Deliberately
/// takes no socket: arriving input remains unobserved until this phase retires.
async fn owned_authorization<F: Future>(
    future: Pin<&mut F>,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
    expiry: Option<i128>,
    source: Option<&SourceConnection>,
) -> Option<F::Output> {
    if *stop.borrow() || revoke.now() {
        return None;
    }
    tokio::select! {biased;
        ()=cancelled(stop)=>None,
        ()=revoke.wait()=>None,
        ()=expired(expiry)=>None,
        ()=async { if let Some(source) = source { source.revoked().await } else { std::future::pending().await } }=>None,
        result=future=>Some(result),
    }
}
async fn wait_delivery<F: Future<Output = std::result::Result<Delivery, SessionError>>>(
    future: F,
    source: &SourceConnection,
    context: &mut Context,
    ws: &mut Option<Socket>,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
    shared: &Shared,
) -> Result<Option<Delivery>> {
    tokio::pin!(future);
    #[cfg(test)]
    let future = futures_util::future::poll_fn(|cx| {
        let polled = future.as_mut().poll(cx);
        if polled.is_pending() {
            shared.source_pending.fetch_add(1, Ordering::AcqRel);
        }
        polled
    });
    #[cfg(test)]
    tokio::pin!(future);
    match work(
        future.as_mut(),
        ws,
        stop,
        revoke,
        source.expiry(),
        Some(source),
        shared,
    )
    .await
    {
        Some(result) => result.map(Some).map_err(failure),
        None => {
            context.phase(6);
            source.revoke();
            drop(ws.take());
            match future.await {
                Ok(delivery) => delivery.discard().map_err(failure)?,
                Err(error) => return Err(failure(error)),
            }
            Ok(None)
        }
    }
}
async fn output(
    mut delivery: Delivery,
    source: &SourceConnection,
    context: &mut Context,
    ws: &mut Option<Socket>,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
    admitted: &crate::auth::AdmittedCredential,
    shared: &Shared,
    caller: &intent_core::Caller,
) -> Result<bool> {
    let ready = futures_util::future::poll_fn(|cx| {
        ws.as_mut().expect("source socket").poll_ready_unpin(cx)
    });
    let mut ready_future = Box::pin(ready);
    // Only this writer can use readiness; no result queue or intervening enqueue.
    let ready = tokio::select! {biased;()=cancelled(stop)=>None,()=revoke.wait()=>None,()=expired(source.expiry())=>None,()=source.revoked()=>None,r=ready_future.as_mut()=>Some(r)};
    drop(ready_future);
    if !matches!(ready, Some(Ok(()))) {
        context.phase(6);
        source.revoke();
        drop(ws.take());
        delivery.discard().map_err(failure)?;
        return Ok(false);
    }
    #[cfg(test)]
    let auth_io = AuthIo::new(shared, ws.as_ref().expect("source socket"));
    let authorization = async {
        #[cfg(test)]
        {
            let gate = shared.page_auth_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.reached.notify_one();
                gate.release.notified().await;
            }
        }
        let credential = admitted.prepared_valid_for(&shared.token, shared.api.as_ref(), caller);
        tokio::pin!(credential);
        #[cfg(test)]
        let credential = futures_util::future::poll_fn(|cx| {
            let result = credential.as_mut().poll(cx);
            if result.is_pending() {
                shared.page_auth_pending.notify_one();
            }
            result
        })
        .await;
        #[cfg(not(test))]
        let credential = credential.await;
        // Preserve exact credential errors and the source authorization future.
        match credential {
            Ok(true) => delivery.authorize().await.map_err(failure),
            Ok(false) => Err(Error::Forbidden("credential revoked".into())),
            Err(error) => Err(error),
        }
    };
    let mut authorization = Box::pin(authorization);
    let authorized = owned_authorization(
        authorization.as_mut(),
        stop,
        revoke,
        source.expiry(),
        Some(source),
    )
    .await;
    let result = match authorized {
        Some(result) => result,
        None => {
            context.phase(6);
            source.revoke();
            drop(ws.take());
            let result = authorization.as_mut().await;
            drop(authorization);
            result?;
            delivery.discard().map_err(failure)?;
            return Ok(false);
        }
    };
    drop(authorization);
    #[cfg(test)]
    drop(auth_io);
    if let Err(error) = result {
        context.phase(6);
        source.revoke();
        drop(ws.take());
        if matches!(error, Error::Internal(_)) {
            delivery.uncertain();
        } else {
            delivery.discard().map_err(failure)?;
        }
        return Err(error);
    }
    if *stop.borrow() || revoke.now() {
        context.phase(6);
        source.revoke();
        drop(ws.take());
        delivery.discard().map_err(failure)?;
        return Ok(false);
    }
    // The Services-owned typed seam holds actual context and operation authority
    // locks across local checks and this adapter's single synchronous start_send.
    if let Err(error) = delivery.enqueue(&mut Writer(ws.as_mut().expect("source socket"))) {
        context.phase(6);
        source.revoke();
        drop(ws.take());
        if error == SessionError::Uncertain {
            delivery.uncertain();
        } else {
            delivery.discard().map_err(failure)?;
        }
        return Err(failure(error));
    }
    let mut flush = Box::pin(ws.as_mut().expect("source socket").flush());
    let settled = tokio::select! {biased;()=cancelled(stop)=>None,()=revoke.wait()=>None,()=expired(source.expiry())=>None,()=source.revoked()=>None,result=&mut flush=>Some(result)};
    let interrupted = settled.is_none();
    if interrupted {
        context.phase(6);
        source.revoke();
    }
    // Even after revocation, own the exact already-accepted local flush until it
    // settles. No timeout/Drop is treated as successful write retirement.
    let result = match settled {
        Some(result) => result,
        None => flush.as_mut().await,
    };
    drop(flush);
    if result.is_err() {
        drop(ws.take());
        delivery.uncertain();
        return Err(Error::Internal("source write settlement uncertain".into()));
    }
    delivery.flushed().map_err(failure)?;
    Ok(!interrupted)
}
async fn control(
    frame: String,
    ws: &mut Option<Socket>,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
    admitted: &crate::auth::AdmittedCredential,
    shared: &Shared,
    caller: &intent_core::Caller,
) -> Result<bool> {
    let mut ready = Box::pin(futures_util::future::poll_fn(|cx| {
        ws.as_mut().expect("control socket").poll_ready_unpin(cx)
    }));
    let result = tokio::select! {biased;()=cancelled(stop)=>None,()=revoke.wait()=>None,result=ready.as_mut()=>Some(result)};
    drop(ready);
    if !matches!(result, Some(Ok(()))) {
        return Ok(false);
    }
    #[cfg(test)]
    let auth_io = AuthIo::new(shared, ws.as_ref().expect("control socket"));
    let mut auth =
        Box::pin(admitted.prepared_valid_for(&shared.token, shared.api.as_ref(), caller));
    #[cfg(test)]
    let mut observed_auth = Box::pin(futures_util::future::poll_fn(|cx| {
        let polled = auth.as_mut().poll(cx);
        if polled.is_pending() {
            shared.control_pending.notify_one();
        }
        polled
    }));
    #[cfg(test)]
    let result = owned_authorization(observed_auth.as_mut(), stop, revoke, None, None).await;
    #[cfg(test)]
    drop(observed_auth);
    #[cfg(not(test))]
    let result = owned_authorization(auth.as_mut(), stop, revoke, None, None).await;
    let Some(result) = result else {
        drop(ws.take());
        return auth.as_mut().await.map(|_| false);
    };
    drop(auth);
    #[cfg(test)]
    drop(auth_io);
    if !result? || *stop.borrow() || revoke.now() {
        return Ok(false);
    }
    #[cfg(test)]
    if shared.partial_control.load(Ordering::Acquire) {
        let meter = &ws
            .as_ref()
            .expect("control socket")
            .get_ref()
            .get_ref()
            .0
            .meter;
        meter
            .fail_write_at
            .store(meter.written.load(Ordering::Acquire) + 8, Ordering::Release);
    }
    ws.as_mut()
        .expect("control socket")
        .start_send_unpin(Message::Text(frame.into()))
        .map_err(internal)?;
    ws.as_mut()
        .expect("control socket")
        .flush()
        .await
        .map_err(internal)?;
    Ok(true)
}
pub(super) async fn run(
    shared: &Shared,
    context: &mut Context,
    ws: &mut Option<Socket>,
    caller: intent_core::Caller,
    admitted: &crate::auth::AdmittedCredential,
    stop: &mut watch::Receiver<bool>,
    revoke: &mut Revocations,
    mut source: SourceConnection,
) -> Result<()> {
    source.attach_transport().map_err(failure)?;
    let result=async { loop {
        let frame=tokio::select!{biased;()=cancelled(stop)=>None,()=revoke.wait()=>None,()=expired(source.expiry())=>None,()=source.revoked()=>None,frame=ws.as_mut().expect("source socket").next()=>frame};
        let Some(Ok(Message::Text(text)))=frame else{break;};
        let request=match wire::parse(&text){Ok(request)=>request,Err(error)=>{source.revoke();return Err(failure(error));}};
        let id=request.id;
        let original_close = matches!(&request.method, wire::Method::Close(_)) && source.expiry().is_some();
        let response=match request.method{
            wire::Method::Open(op)=>match source.open(op,id.clone()){
                Ok(Open::Pending(pending))=>{
                    let Some(delivery)=wait_delivery(pending,&source,context,ws,stop,revoke,shared).await? else{break;};
                    if !output(delivery,&source,context,ws,stop,revoke,admitted,shared,&caller).await?{break;}continue;
                }
                Ok(Open::Existing(control))=>wire::control(&control,&id),Err(error)=>wire::error(error,&id),
            },
            wire::Method::Read(read)=>match source.read(read,id.clone()){
                Ok(pending)=>{
                    let Some(delivery)=wait_delivery(pending,&source,context,ws,stop,revoke,shared).await? else{break;};
                    if !output(delivery,&source,context,ws,stop,revoke,admitted,shared,&caller).await?{break;}continue;
                }
                Err(error)=>wire::error(error,&id),
            },
            wire::Method::Close(op)=>match source.close(op){Ok(control)=>wire::control(&control,&id),Err(error)=>wire::error(error,&id)},
        }.map_err(failure)?;
        if !control(response,ws,stop,revoke,admitted,shared,&caller).await? || original_close {break;}
    }
    Ok::<_,Error>(()) }.await;
    context.phase(6);
    if matches!(&result, Err(Error::Internal(_))) {
        // Match the outer Context's uncertainty policy BEFORE clearing original
        // transport debt. A dropped socket cannot turn this error into a receipt.
        source.revoke();
        drop(ws.take());
        // Attached SourceConnection::drop marks its exact owner uncertain;
        // transport_retired is deliberately never called for this outcome.
        drop(source);
        return result;
    }
    source.revoke();
    drop(ws.take());
    source.transport_retired().map_err(failure)?;
    source.retire().await.map_err(failure)?;
    result
}
