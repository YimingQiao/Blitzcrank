use crate::{ensure, Result};
use delayed_coding::Model;

pub(crate) struct Group {
    pub columns: Vec<usize>,
    pub radices: Vec<u32>,
    pub model: Model,
}
pub(crate) fn build(
    input: &[Option<(&[u16], usize)>],
    rows: usize,
    bits: u32,
) -> Result<Vec<Group>> {
    let mut groups = Vec::new();
    let mut c = 0;
    while c < input.len() {
        if input[c].is_none() {
            c += 1;
            continue;
        }
        let mut columns = Vec::new();
        let mut radices = Vec::new();
        let mut product = 1;
        while c < input.len() && columns.len() < 4 {
            let Some((_, radix)) = input[c] else {
                break;
            };
            if product * radix > 65536 {
                break;
            }
            product *= radix;
            columns.push(c);
            radices.push(radix as u32);
            c += 1;
        }
        let mut counts = vec![0; product];
        for row in 0..rows {
            let mut id = 0;
            for (&c, &radix) in columns.iter().zip(&radices) {
                id = id * radix + u32::from(input[c].unwrap().0[row]);
            }
            counts[id as usize] += 1;
        }
        groups.push(Group {
            columns,
            radices,
            model: crate::probability_model(&counts, bits)?,
        });
    }
    Ok(groups)
}
pub(crate) fn write(groups: &[Group], out: &mut Vec<u8>) {
    out.extend_from_slice(&(groups.len() as u32).to_le_bytes());
    for g in groups {
        out.push(g.columns.len() as u8);
        for &c in &g.columns {
            out.extend_from_slice(&(c as u16).to_le_bytes());
        }
        let frequencies = g.model.frequencies();
        out.extend_from_slice(&(frequencies.len() as u32).to_le_bytes());
        for f in frequencies {
            out.extend_from_slice(&f.to_le_bytes());
        }
    }
}
pub(crate) fn read(input: &[u8], sizes: &[Option<usize>]) -> Result<(Vec<Group>, usize)> {
    struct Reader<'a> {
        input: &'a [u8],
        p: usize,
    }
    impl Reader<'_> {
        fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
            let bytes = self
                .input
                .get(self.p..self.p + N)
                .ok_or("truncated joint model")?;
            self.p += N;
            Ok(bytes.try_into()?)
        }
        fn word(&mut self) -> Result<usize> {
            Ok(u32::from_le_bytes(self.take()?) as usize)
        }
    }
    let mut r = Reader { input, p: 0 };
    let n = r.word()?;
    ensure(n <= sizes.len(), "invalid group count")?;
    let mut seen = vec![false; sizes.len()];
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let count = r.take::<1>()?[0] as usize;
        ensure((1..=4).contains(&count), "invalid group width")?;
        let mut columns = Vec::new();
        let mut radices = Vec::new();
        let mut product = 1usize;
        for _ in 0..count {
            let c = u16::from_le_bytes(r.take()?) as usize;
            ensure(c < sizes.len() && !seen[c], "invalid/repeated group column")?;
            let radix = sizes[c].ok_or("invalid joint column type")?;
            seen[c] = true;
            columns.push(c);
            radices.push(radix as u32);
            product = product
                .checked_mul(radix)
                .ok_or("group alphabet overflow")?;
            ensure(product <= 65536, "group alphabet limit")?;
        }
        ensure(
            r.word()? == product && product <= (input.len() - r.p) / 4,
            "invalid group frequencies",
        )?;
        let frequencies: Vec<_> = (0..product)
            .map(|_| Ok(r.word()? as u32))
            .collect::<Result<_>>()?;
        groups.push(Group {
            columns,
            radices,
            model: crate::decoding_model(Model::new(&frequencies)?),
        });
    }
    ensure(
        sizes
            .iter()
            .enumerate()
            .all(|(i, s)| s.is_none() || seen[i]),
        "missing joint column",
    )?;
    Ok((groups, r.p))
}

pub(crate) fn mapping(group: &Group) -> crate::record::Mapping {
    let decoded = (0..group.model.alphabet_size())
        .map(|id| {
            let mut id = id as u32;
            let mut values = [0; 4];
            for (i, &radix) in group.radices.iter().enumerate().rev() {
                values[i] = (id % radix) as u16;
                id /= radix;
            }
            values
        })
        .collect();
    crate::record::Mapping::Group {
        columns: group.columns.clone(),
        decoded,
    }
}
