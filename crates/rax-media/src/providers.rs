pub trait OCRProvider {
    fn ocr(&self, input: &str) -> String;
}

pub trait CaptionProvider {
    fn caption(&self, input: &str) -> String;
}

pub trait TranscriptProvider {
    fn transcript(&self, input: &str) -> String;
}
