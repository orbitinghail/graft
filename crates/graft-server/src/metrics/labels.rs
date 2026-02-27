use measured::LabelGroup;

#[derive(measured::FixedCardinalityLabel, Copy, Clone)]
pub enum ResultLabel {
    Success = 0,
    Failure = 1,
}

#[derive(LabelGroup)]
#[label(set=ResultLabelSet)]
pub struct ResultLabelGroup {
    result: ResultLabel,
}

impl<T, E> From<&Result<T, E>> for ResultLabelGroup {
    fn from(result: &Result<T, E>) -> Self {
        match result {
            Ok(_) => ResultLabelGroup { result: ResultLabel::Success },
            Err(_) => ResultLabelGroup { result: ResultLabel::Failure },
        }
    }
}
