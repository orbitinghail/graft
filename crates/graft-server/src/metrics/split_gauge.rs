use std::io::Write;

use measured::{
    metric::{
        counter::CounterState,
        group::Encoding,
        name::{MetricNameEncoder, Suffix},
        Metric, MetricEncoding, MetricLockGuard,
    },
    text::{MetricType, TextEncoder},
};

pub type SplitGauge = Metric<SplitGaugeState>;

/// SplitGaugeExt provides a set of convenience methods for working with SplitGauge metrics.
pub trait SplitGaugeExt {
    fn inc(&self);
    fn inc_by(&self, value: u64);
    fn dec(&self);
    fn dec_by(&self, value: u64);
    fn guard(&self) -> SplitGaugeGuard<'_>;
}

impl SplitGaugeExt for SplitGauge {
    #[inline]
    fn inc(&self) {
        self.inc_by(1);
    }

    #[inline]
    fn inc_by(&self, value: u64) {
        self.get_metric().inc_by(value);
    }

    #[inline]
    fn dec(&self) {
        self.dec_by(1);
    }

    #[inline]
    fn dec_by(&self, value: u64) {
        self.get_metric().dec_by(value);
    }

    #[inline]
    fn guard(&self) -> SplitGaugeGuard<'_> {
        let state = self.get_metric();
        state.inc();
        SplitGaugeGuard { state }
    }
}

#[derive(Default)]
pub struct SplitGaugeState {
    inc: CounterState,
    dec: CounterState,
}

impl SplitGaugeState {
    #[inline]
    pub fn inc(&self) {
        self.inc.inc();
    }

    #[inline]
    pub fn inc_by(&self, value: u64) {
        self.inc.inc_by(value);
    }

    #[inline]
    pub fn dec(&self) {
        self.dec.inc();
    }

    #[inline]
    pub fn dec_by(&self, value: u64) {
        self.dec.inc_by(value);
    }
}

impl measured::metric::MetricType for SplitGaugeState {
    type Metadata = ();
}

pub struct Inc;
const INC_SUFFIX: &[u8] = b"_inc";

impl Suffix for Inc {
    #[inline]
    fn encode_text(&self, b: &mut impl Write) -> std::io::Result<()> {
        b.write_all(INC_SUFFIX)
    }

    #[inline]
    fn encode_len(&self) -> usize {
        INC_SUFFIX.len()
    }
}

pub struct Dec;
const DEC_SUFFIX: &[u8] = b"_dec";

impl Suffix for Dec {
    #[inline]
    fn encode_text(&self, b: &mut impl Write) -> std::io::Result<()> {
        b.write_all(DEC_SUFFIX)
    }

    #[inline]
    fn encode_len(&self) -> usize {
        DEC_SUFFIX.len()
    }
}

impl<W: Write> MetricEncoding<TextEncoder<W>> for SplitGaugeState {
    fn write_type(
        name: impl MetricNameEncoder,
        enc: &mut TextEncoder<W>,
    ) -> Result<(), <TextEncoder<W> as Encoding>::Err> {
        enc.write_type(&name.by_ref().with_suffix(Dec), MetricType::Counter)?;
        enc.write_type(&name.by_ref().with_suffix(Inc), MetricType::Counter)?;
        Ok(())
    }

    fn collect_into(
        &self,
        _metadata: &Self::Metadata,
        labels: impl measured::LabelGroup,
        name: impl MetricNameEncoder,
        enc: &mut TextEncoder<W>,
    ) -> Result<(), <TextEncoder<W> as Encoding>::Err> {
        self.dec
            .collect_into(&(), labels.by_ref(), name.by_ref().with_suffix(Dec), enc)?;
        self.dec
            .collect_into(&(), labels.by_ref(), name.by_ref().with_suffix(Inc), enc)?;
        Ok(())
    }
}

/// SplitGaugeGuard is a guard that decrements the SplitGauge metric when dropped.
pub struct SplitGaugeGuard<'a> {
    state: MetricLockGuard<'a, SplitGaugeState>,
}

impl<'a> Drop for SplitGaugeGuard<'a> {
    fn drop(&mut self) {
        self.state.dec();
    }
}
