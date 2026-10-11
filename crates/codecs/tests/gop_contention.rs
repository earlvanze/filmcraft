//! A bounded model of a decoder/rotation owner waiting for a nested Rayon request.
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

use filmcraft_codecs::{
    gop::{GopCache, VideoSamples},
    video::{DecodedFrame, VideoDecoder},
};
use filmcraft_frame::VideoFrame;

struct Samples {
    first: AtomicBool,
    entered: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

struct Decoder {
    block: bool,
    entered: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl VideoDecoder for Decoder {
    fn decode(&mut self, _: &[u8], pts: i64) -> filmcraft_codecs::Result<Vec<DecodedFrame>> {
        if self.block {
            self.entered.send(()).unwrap();
            let (lock, cv) = &*self.release;
            let guard = lock.lock().unwrap();
            // Even the negative control releases its owner, rather than hanging the test runner.
            let _released = cv.wait_timeout_while(guard, Duration::from_secs(5), |released| !*released).unwrap();
        }
        Ok(vec![DecodedFrame { pts, frame: VideoFrame::rgba8(2, 1, vec![1, 2, 3, 255, 4, 5, 6, 255]), draft: false }])
    }
    fn flush(&mut self) -> Vec<DecodedFrame> {
        Vec::new()
    }
    fn reset(&mut self) {}
    fn name(&self) -> &str {
        "bounded contention fixture"
    }
}

impl VideoSamples for Samples {
    fn count(&self) -> usize {
        1
    }
    fn pts(&self, _: usize) -> i64 {
        0
    }
    fn sync_before(&self, _: usize) -> usize {
        0
    }
    fn sample_at(&self, _: i64) -> Option<usize> {
        Some(0)
    }
    fn read(&self, _: usize) -> filmcraft_codecs::Result<Vec<u8>> {
        Ok(vec![0])
    }
    fn make_decoder(&self) -> filmcraft_codecs::Result<Box<dyn VideoDecoder>> {
        Ok(Box::new(Decoder { block: self.first.swap(false, Ordering::SeqCst), entered: self.entered.clone(), release: self.release.clone() }))
    }
}

#[test]
fn rayon_request_does_not_wait_for_another_threads_decoder_lock() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let samples = Arc::new(Samples { first: AtomicBool::new(true), entered: entered_tx, release: release.clone() });
    let cache = Arc::new(GopCache::new(None).with_rotation(1));
    let owner = {
        let samples = samples.clone();
        let cache = cache.clone();
        std::thread::spawn(move || cache.frame(&*samples, 0).unwrap())
    };
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let requester = std::thread::spawn(move || {
        rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap().install(|| {
            started_tx.send(()).unwrap();
            let result = cache.frame(&*samples, 0);
            done_tx.send(result).unwrap();
        });
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = done_rx.recv_timeout(Duration::from_secs(1));
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    let expected = owner.join().unwrap();
    requester.join().unwrap();
    let actual = early.expect("a Rayon request blocked on a decoder owner that may be waiting for that request").unwrap();
    assert_eq!((actual.width, actual.height), (1, 2));
    assert_eq!(actual.to_rgba8(), expected.to_rgba8());
}
