use std::io::Write;

use measured::{
    label::LabelGroupSet,
    metric::{
        counter::CounterState,
        group::Encoding,
        name::{MetricNameEncoder, Suffix},
        Metric, MetricEncoding, MetricVec,
    },
    text::{MetricType, TextEncoder},
};

pub type SplitGauge = Metric<SplitGaugeState>;
pub type SplitGaugeVec<L> = MetricVec<SplitGaugeState, L>;

/// SplitGaugeExt provides a set of convenience methods for working with SplitGauge metrics.
pub trait SplitGaugeExt {
    fn inc(&self);
    fn inc_by(&self, value: u64);
    fn dec(&self);
    fn dec_by(&self, value: u64);
}

pub trait SplitGaugeVecExt<L: LabelGroupSet> {
    fn inc(&self, label: L::Group<'_>);
    fn inc_by(&self, label: L::Group<'_>, value: u64);
    fn dec(&self, label: L::Group<'_>);
    fn dec_by(&self, label: L::Group<'_>, value: u64);
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
}

impl<L: LabelGroupSet> SplitGaugeVecExt<L> for SplitGaugeVec<L> {
    #[inline]
    fn inc(&self, label: <L as LabelGroupSet>::Group<'_>) {
        self.inc_by(label, 1);
    }

    #[inline]
    fn inc_by(&self, label: <L as LabelGroupSet>::Group<'_>, value: u64) {
        self.get_metric(self.with_labels(label)).inc_by(value);
    }

    #[inline]
    fn dec(&self, label: <L as LabelGroupSet>::Group<'_>) {
        self.dec_by(label, 1);
    }

    #[inline]
    fn dec_by(&self, label: <L as LabelGroupSet>::Group<'_>, value: u64) {
        self.get_metric(self.with_labels(label)).dec_by(value);
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
