use core::slice::Iter;
use std::iter::Peekable;

use anyhow::anyhow;

use crate::tokenizer::Token;

#[derive(Debug, Clone)]
pub enum ParserValue {
    SimpleString(String),
    BulkString(String),
    Array(Vec<ParserValue>),
    NullBulkString,
}

impl ParserValue {
    pub fn is_string(self: &ParserValue) -> bool {
        matches!(self, ParserValue::SimpleString(_)) || matches!(self, ParserValue::BulkString(_))
    }

    pub fn is_array(self: &ParserValue) -> bool {
        matches!(self, ParserValue::Array(_))
    }

    pub fn to_string(self: &ParserValue) -> Option<String> {
        match self {
            ParserValue::SimpleString(s) => Some(s.clone()),
            ParserValue::BulkString(s) => Some(s.clone()),
            _ => None,
        }
    }

    pub fn to_vec(self: &ParserValue) -> Option<&Vec<ParserValue>> {
        match self {
            ParserValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn to_tokens(self: &ParserValue) -> Vec<Token> {
        match self {
            ParserValue::SimpleString(s) => {
                vec![Token::Plus, Token::String(s.clone()), Token::Separator]
            }
            ParserValue::BulkString(s) => {
                vec![
                    Token::Dollar,
                    Token::Number(s.len() as i64),
                    Token::Separator,
                    Token::String(s.clone()),
                    Token::Separator,
                ]
            }
            ParserValue::Array(arr) => {
                let mut tokens = vec![
                    Token::Asterisk,
                    Token::Number(arr.len() as i64),
                    Token::Separator,
                ];
                for parser_value in arr {
                    tokens.append(&mut parser_value.to_tokens());
                }
                tokens
            }
            ParserValue::NullBulkString => {
                vec![Token::Dollar, Token::Number(-1), Token::Separator]
            }
        }
    }
}

pub fn parse_tokens(tokens: &[Token]) -> Option<ParserValue> {
    if tokens.is_empty() {
        return None;
    }

    let mut tokens_iter = tokens.iter().peekable();
    let first = tokens_iter.peek().expect("must have at least one token");

    eprintln!("First Token {:?}", first);

    match first {
        // Simple String
        Token::Plus => {
            if let Ok(simple_string) = tokens_to_simple_string(&mut tokens_iter) {
                return Some(simple_string);
            }

            None
        }
        // Bulk String
        Token::Dollar => {
            if let Ok(bulk_string) = tokens_to_bulk_string(&mut tokens_iter) {
                return Some(bulk_string);
            }

            None
        }
        // Array
        Token::Asterisk => match tokens_to_array(&mut tokens_iter) {
            Ok(arr) => Some(arr),
            Err(err) => {
                eprintln!("{:?}", err);
                None
            }
        },
        _ => None,
    }
}

fn tokens_to_simple_string(token_iter: &mut Peekable<Iter<Token>>) -> anyhow::Result<ParserValue> {
    if !token_iter.next().is_some_and(|t| t.is_plus()) {
        return Err(anyhow!("first token in simple string must be a plus"));
    }
    let str_token = token_iter
        .next()
        .expect("should have a second token for simple string");

    let separator_token = token_iter
        .next()
        .expect("should have a third token for simple string");

    if !separator_token.is_separator() {
        return Err(anyhow!("third token in simple string must be a separator"));
    }

    Ok(ParserValue::SimpleString(str_token.to_string().expect(
        "should be able to get strings from string tokens",
    )))
}

fn tokens_to_bulk_string(token_iter: &mut Peekable<Iter<Token>>) -> anyhow::Result<ParserValue> {
    if !token_iter.next().is_some_and(|t| t.is_dollar()) {
        return Err(anyhow!("first token in bulk string must be a dollar sign"));
    }
    let size_token = token_iter
        .next()
        .expect("should have  second token for bulk string");
    if !size_token.is_number() {
        return Err(anyhow!("second token in bulk string must be a number"));
    }
    let separator_token = token_iter
        .next()
        .expect("should have a third token for simple string");
    if !separator_token.is_separator() {
        return Err(anyhow!("third token in bulk string must be a separator"));
    }
    let mut str_tokens = Vec::new();
    while token_iter.peek().is_some_and(|t| !t.is_separator()) {
        let str_token = token_iter.next().expect("should str_token");
        str_tokens.push(str_token);
    }

    let separator_token = token_iter
        .next()
        .expect("should have a fifth token for simple string");
    if !separator_token.is_separator() {
        return Err(anyhow!("fifth token in bulk string must be a separator"));
    }
    let mut s = String::with_capacity(size_token.to_usize().expect("size_token must be a usize"));
    for t in str_tokens.iter() {
        match t {
            Token::Plus => {
                s.push('+');
            }
            Token::Hyphen => {
                s.push('-');
            }
            Token::Colon => {
                s.push(':');
            }
            Token::Dollar => {
                s.push('$');
            }
            Token::Asterisk => {
                s.push('*');
            }
            Token::Underscore => {
                s.push('_');
            }
            Token::PoundSign => {
                s.push('#');
            }
            Token::Comma => {
                s.push(',');
            }
            Token::LeftParenthesis => {
                s.push('(');
            }
            Token::Exclamation => {
                s.push('!');
            }
            Token::Equals => {
                s.push('=');
            }
            Token::Percentage => {
                s.push('%');
            }
            Token::Tilda => {
                s.push('~');
            }
            Token::GreaterThan => {
                s.push('>');
            }
            Token::String(ts) => s.push_str(ts),
            Token::Number(n) => s.push_str(n.to_string().as_str()),
            Token::Separator => {}
        }
    }
    if s.len() != size_token.to_usize().expect("size_token must be a usize") {
        return Err(anyhow!("incorrect string size in bulk token"));
    }

    Ok(ParserValue::BulkString(s))
}

fn tokens_to_array(token_iter: &mut Peekable<Iter<Token>>) -> anyhow::Result<ParserValue> {
    if !token_iter.next().is_some_and(|t| t.is_asterisk()) {
        return Err(anyhow!("first token in bulk string must be an asterisk"));
    }
    let length = token_iter.next().expect("should have a length token");
    eprintln!("Length Token: {:?}", length);
    if !length.is_number() {
        return Err(anyhow!("second token in array should be length"));
    }
    let length = length.to_i64().expect("number token should have i64");
    eprintln!("Length: {:?}", length);
    if length < 0 {
        return Err(anyhow!("array length cannot be negative"));
    }

    if length == 0 {
        return Ok(ParserValue::Array(Vec::new()));
    }

    if !token_iter.next().is_some_and(|t| t.is_separator()) {
        return Err(anyhow!("third token in an array must be a separator"));
    }

    let mut values: Vec<ParserValue> = Vec::with_capacity(length as usize);
    for _ in 0..length {
        let first = token_iter.peek().expect("should have next token in array");
        eprintln!("First Array Token: {:?}", first);
        match first {
            Token::Plus => {
                let simple_string = tokens_to_simple_string(token_iter);
                if let Ok(simple_string) = simple_string {
                    values.push(simple_string);
                } else {
                    return Err(simple_string.err().unwrap());
                }
            }
            Token::Dollar => {
                let bulk_string = tokens_to_bulk_string(token_iter);
                if let Ok(bulk_string) = bulk_string {
                    values.push(bulk_string);
                } else {
                    let err = bulk_string.err().unwrap();
                    eprintln!("{:?}", err);
                    return Err(err);
                }
            }
            Token::Asterisk => {
                let arr = tokens_to_array(token_iter);
                if let Ok(arr) = arr {
                    values.push(arr);
                } else {
                    return Err(arr.err().unwrap());
                }
            }
            _ => return Err(anyhow!("unexpected starting token in array")),
        }
    }

    Ok(ParserValue::Array(values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parses_bulk_string_with_negative_number() {
        let tokens = [
            Token::Dollar,
            Token::Number(2),
            Token::Separator,
            Token::Hyphen,
            Token::Number(1),
            Token::Separator,
        ];
        let result = tokens_to_bulk_string(&mut tokens.iter().peekable());
        assert!(result.is_ok());
        assert_eq!(
            ParserValue::BulkString("-1".to_string())
                .to_string()
                .unwrap(),
            result.unwrap().to_string().unwrap()
        );
    }

    #[test]
    fn test_parses_bulk_string() {
        let tokens = [
            Token::Dollar,
            Token::Number(5),
            Token::Separator,
            Token::String("PSYNC".to_string()),
            Token::Separator,
        ];

        let result = tokens_to_bulk_string(&mut tokens.iter().peekable());
        assert!(result.is_ok());
        assert_eq!(
            ParserValue::BulkString("PSYNC".to_string())
                .to_string()
                .unwrap(),
            result.unwrap().to_string().unwrap()
        );
    }
}
