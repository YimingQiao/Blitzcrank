//! Exact fixed-scale decimal spelling, not floating-point quantization.
pub(crate) fn parse(token: &[u8]) -> Option<(i64, u8)> {
    let negative = token.first() == Some(&b'-');
    let digits = if negative { &token[1..] } else { token };
    let dot = digits.iter().position(|&b| b == b'.');
    let whole = dot.unwrap_or(digits.len());
    if whole == 0 || (whole > 1 && digits[0] == b'0') {
        return None;
    }
    let scale = dot.map_or(0, |d| digits.len() - d - 1);
    if scale > 18 || dot.is_some() && scale == 0 {
        return None;
    }
    let mut value = 0u64;
    for (i, &b) in digits.iter().enumerate() {
        if Some(i) == dot {
            continue;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    if negative && value == 0 {
        return None; // Keep the sign of negative zero in the lexical backend.
    }
    let signed = if negative {
        if value > 1u64 << 63 {
            return None;
        }
        (value as i64).wrapping_neg()
    } else {
        i64::try_from(value).ok()?
    };
    Some((signed, scale as u8))
}

pub(crate) fn append(out: &mut Vec<u8>, value: i64, scale: u8) {
    if scale == 0 {
        crate::table::append_integer(out, value);
        return;
    }
    let mut digits = [b'0'; 40];
    let mut position = digits.len();
    let mut magnitude = value.unsigned_abs();
    for _ in 0..scale {
        position -= 1;
        digits[position] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
    }
    position -= 1;
    digits[position] = b'.';
    loop {
        position -= 1;
        digits[position] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if value < 0 {
        position -= 1;
        digits[position] = b'-';
    }
    out.extend_from_slice(&digits[position..]);
}

#[cfg(test)]
mod tests {
    #[test]
    fn exact_decimal_spelling() {
        for value in [
            i64::MIN,
            i64::MIN + 1,
            -1234567,
            -1,
            0,
            1,
            1234567,
            i64::MAX,
        ] {
            for scale in 0..=18 {
                let mut text = Vec::new();
                super::append(&mut text, value, scale);
                assert_eq!(super::parse(&text), Some((value, scale)));
            }
        }
        for token in [
            "",
            "-0",
            "-0.000",
            "+1",
            "01",
            "1.",
            ".1",
            "1e2",
            "1.2.3",
            "null",
            "9223372036854775808",
            "0.0000000000000000001",
        ] {
            assert!(super::parse(token.as_bytes()).is_none(), "{token}");
        }
    }
}
