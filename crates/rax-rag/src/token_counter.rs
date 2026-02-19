pub fn count_tokens(text: &str) -> usize {
    text.split_whitespace().count()
}
