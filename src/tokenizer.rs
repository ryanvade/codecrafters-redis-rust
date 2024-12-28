use std::str;

use anyhow::anyhow;

use crate::tokenizer::Token::Separator;

#[derive(Debug, Clone)]
pub enum Token {
    Plus,
    Hyphen,
    Colon,
    Dollar,
    Asterisk,
    Underscore,
    PoundSign,
    Comma,
    LeftParenthesis,
    Exclamation,
    Equals,
    Percentage,
    Tilda,
    GreaterThan,
    String(String),
    Number(i64),
    Separator,
}

impl Token {
    pub fn is_separator(self: &Token) -> bool {
        matches!(self, Separator)
    }

    pub fn is_string(self: &Token) -> bool {
        matches!(self, Token::String(_))
    }

    pub fn is_plus(self: &Token) -> bool {
        matches!(self, Token::Plus)
    }

    pub fn is_dollar(self: &Token) -> bool {
        matches!(self, Token::Dollar)
    }

    pub fn is_number(self: &Token) -> bool {
        matches!(self, Token::Number(_))
    }

    pub fn is_hyphen(self: &Token) -> bool {
        matches!(self, Token::Hyphen)
    }

    pub fn is_asterisk(self: &Token) -> bool {
        matches!(self, Token::Asterisk)
    }

    pub fn to_string(self: &Token) -> Option<String> {
        match self {
            Token::String(s) => Some(s.clone()),
            Token::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }

    pub fn to_i64(self: &Token) -> Option<i64> {
        match self {
            Token::Number(n) => Some(n.clone()),
            _ => None,
        }
    }

    pub fn to_usize(self: &Token) -> Option<usize> {
        match self {
            Token::Number(n) => n.clone().try_into().ok(),
            _ => None,
        }
    }
}

pub fn parse_resp_tokens_from_str(input: &str) -> anyhow::Result<Vec<Token>> {
    let mut tokens: Vec<Token> = Vec::new();
    let mut iter = input.chars().peekable();

    while let Some(ch) = iter.next() {
        match ch {
            '+' => tokens.push(Token::Plus),
            '-' => tokens.push(Token::Hyphen),
            ':' => tokens.push(Token::Colon),
            '$' => tokens.push(Token::Dollar),
            '*' => tokens.push(Token::Asterisk),
            '_' => tokens.push(Token::Underscore),
            '#' => tokens.push(Token::PoundSign),
            ',' => tokens.push(Token::Comma),
            '(' => tokens.push(Token::LeftParenthesis),
            '!' => tokens.push(Token::Exclamation),
            '=' => tokens.push(Token::Equals),
            '%' => tokens.push(Token::Percentage),
            '~' => tokens.push(Token::Tilda),
            '>' => tokens.push(Token::GreaterThan),
            '0'..='9' => {
                // TODO: Support BIG numbers
                let mut s = String::from(ch);
                let mut rest = Vec::<char>::new();
                while iter.peek().is_some_and(|c| c.is_ascii_digit()) {
                    let c = iter.next().unwrap();
                    rest.push(c)
                }
                if !rest.is_empty() {
                    let rest = rest.iter().collect::<String>();
                    s = s + &rest;
                }

                tokens.push(Token::Number(s.parse().unwrap()));
            }
            '\r' => {
                if iter.peek().is_some_and(|s| *s == '\n') {
                    let _ = iter.next();
                    tokens.push(Separator);
                } else {
                    let mut tmp = [0; 4];
                    let s = ch.encode_utf8(&mut tmp);
                    tokens.push(Token::String(s.to_string()));
                }
            }
            _ => {
                let mut s: String = ch.to_string();
                while let Some(curr) = iter.next() {
                    if curr == '\r' && iter.by_ref().peek().is_some_and(|s| *s == '\n') {
                        tokens.push(Token::String(s.clone()));
                        tokens.push(Separator);
                        let _ = iter.next();
                        break;
                    } else {
                        s.push(curr);
                    }
                }
            }
        };
    }

    Ok(tokens)
}

pub fn serialize_tokens(tokens: &Vec<Token>) -> anyhow::Result<String> {
    if tokens.len() < 1 {
        return Err(anyhow!("cannot serialize empty vector of tokens"));
    }

    let mut chars: Vec<char> = Vec::new();
    for token in tokens {
        match token {
            Token::Number(n) => chars.append(&mut n.to_string().chars().collect::<Vec<char>>()),
            Token::Asterisk => chars.push('*'),
            Token::Dollar => chars.push('$'),
            Token::String(s) => chars.append(&mut s.as_str().chars().collect::<Vec<char>>()),
            Token::Plus => chars.push('+'),
            Separator => {
                chars.push('\r');
                chars.push('\n');
            }
            Token::GreaterThan => chars.push('>'),
            Token::Tilda => chars.push('~'),
            Token::Percentage => chars.push('%'),
            Token::Equals => chars.push('='),
            Token::Exclamation => chars.push('!'),
            Token::LeftParenthesis => chars.push('('),
            Token::Comma => chars.push(','),
            Token::PoundSign => chars.push('#'),
            Token::Underscore => chars.push('_'),
            Token::Colon => chars.push(':'),
            Token::Hyphen => chars.push('-'),
        }
    }

    let s = chars.into_iter().collect::<String>();

    eprintln!("Serialized Tokens: {:?}", s);

    return Ok(s);
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::parse_resp_tokens_from_str;

    #[test]
    fn test_parses_simple_strings() {
        let input_string = "+OK\r\n";
        let tokens = parse_resp_tokens_from_str(input_string);
        assert!(tokens.is_ok());
        let tokens = tokens.unwrap();
        assert_eq!(3, tokens.len());
        let token = tokens.first().unwrap();
        assert!(token.is_plus());
        let token = tokens.get(1).unwrap();
        assert!(token.is_string());
        let token = token.to_string().unwrap();
        assert_eq!("OK".to_string(), token);
        let token = tokens.get(2).unwrap();
        assert!(token.is_separator());
    }
}
