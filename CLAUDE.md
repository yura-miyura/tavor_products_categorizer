# Tavor fastener matching

Match vendor catalogues against tavor's internal product list (`tavor.csv`).
Each vendor lives in its own folder with a `*_filter.py` script that reads
the vendor file + `tavor.csv` and produces two xlsx outputs.

## Layout

```
tavor.csv                       # base catalogue (~91k rows, comma-delim)
king_data.csv                   # king's catalogue (semi-colon, latin-1)
eurobolt.xlsx                   # eurobolt price list
king/king_filter.py             # tavor + king → tavor_with_king.xlsx
eurobolt/eurobolt_filter.py     # tavor + eurobolt → tavor_with_eurobolt.xlsx
tavor_categorizer_app.py        # older script matching a different file ('baza')
                                # — useful only as a reference for the principle
```

Each script runs from the repo root (`.venv/bin/python king/king_filter.py`)
and writes its outputs in the same directory:

| Script                | Adds columns to tavor                  | Also writes                |
|-----------------------|----------------------------------------|----------------------------|
| king_filter.py        | King Name, King Size, King Code        | king_unmatched.xlsx        |
| eurobolt_filter.py    | Eurobolt Name, Eurobolt Article        | eurobolt_unmatched.xlsx    |

## Tavor schema (the column we key on)

Columns: `Article code, Item Full Name, Item Full Name English, Standard Name,
Standard Code, Standard Group, Item article, Metric thread M, Thread pitch,
Length, Coating, Material, Option, Packaging`.

Quirks:
- `Article code` is a 6-digit zero-padded string. Always read as `dtype=str`;
  write with `num_format='000000'`.
- `Metric thread M` is empty for self-tappers — diameter lives inside
  `Item Full Name` as `ST<d>x<L>`. Both scripts have a `diameter_from_name`
  fallback.
- `Material` carries property class (8.8, 10.9), grade (A2, A2-70, A4-80),
  hardness (140HV, 200HV), or polymer (PA6, NY). A blank `Material` is
  meaningful — it shows up for some self-tappers.
- `Coating` empty = "no coating" (treat as `""`, not as missing data).
- `Standard Code` glues a recess/form suffix to the number: `7981C` (cross),
  `7981F`, `7380-1`/`7380-2`, `912FT` (fully threaded), `~6921` (similar to),
  `934 LH` (left hand), `934 FP` (fine pitch — only on king side).
- `Thread pitch` is populated only on fine-pitch items. Coarse items leave
  it blank even though they have a pitch.

## Vendor schemas

**king_data.csv** (semicolon, latin-1):
`CUSTOMER CODE; MATERIAL; DIN; DESCRIPTION; SIZES; KING CODE; OUR CODE`
- Structured. `DIN` field is `"<family> <code>"` (`DIN 933`, `ISO 7380 F`).
- `SIZES` is `thread X length` (`5X25`) for bolts; bare `thread` for nuts/
  washers; `thread X pitch` for FP nut entries.
- Catalogue is stainless-only (A1/A2/A4/A5/AISI). Coated tavor rows skip the
  matcher entirely.
- File ships with non-UTF8 bytes (German ß lives as 0xc3 0x59). Read with
  `encoding='latin-1'`.

**eurobolt.xlsx**:
- Everything's in the `Номенклатура` free-text field. Other columns are
  Артикул (EU-XXXXXXXX), unit, price, stock.
- The file is also slightly malformed: `xl/SharedStrings.xml` (capital S)
  instead of the lowercase name openpyxl wants. `eurobolt_filter.py` has
  a `_open_xlsx_with_case_fix` that repacks a temp copy.
- Names mix Cyrillic letters that look identical to Latin (М=U+041C,
  А=U+0410, х=U+0445). `latinize()` converts before regex matching.

## Matching pipeline

Both scripts share the same shape:

1. **Build vendor index**: parse each vendor row into
   `(std_name, std_code, material, coating, thread, length, pitch)` →
   `(name, article/code, ...)`. Use `setdefault` so first-write wins on
   collisions (deterministic).
2. **Expand variants on lookup**: a tavor row may use a more specific
   `Standard Code` than the vendor does (`7981C` vs `7981`). Generate
   variants on the tavor side and try each.
3. **Material variants**: tavor's `A2-70` should find vendor's `A2`. King
   does this via single-sided expansion (tavor variants only); eurobolt
   does it on both sides (eurobolt index also registers under stripped
   `A2-70 → A2`).
4. **Dedup to one-to-one**: each vendor code can only own one tavor row.
   Pick the winner with `material_rank()` — plain `A2/A4` wins outright;
   among graded variants, `A2-70/A4-70/A4-80` (the king-A2 equivalent)
   beats `A2-50/A2-80`.
5. **Write xlsx**: format article column as `000000`, widen the new
   vendor-named columns for readability.

### Std-code variants used on the tavor side

| Tavor code        | Tries                          | Why                                      |
|-------------------|--------------------------------|------------------------------------------|
| `912FT`           | `912FT`, `912`                 | FT = fully threaded; vendor doesn't split |
| `7380-1`          | `7380-1`, `7380`               | ISO button head plain                     |
| `7380-2`          | `7380-2`, `7380 F`             | ISO button head with flange               |
| `7981C`           | `7981C`, `7981 C`, `7981`      | Cross recess (king default has no suffix) |
| `7981F`           | `7981F`, `7981 F`              | F-form variant exists on king side        |
| `125A`/`125B`     | spaced + bare                  | DIN 125 A/B map to bare DIN 125           |
| `444B`            | `444 B`, `444`                 | bare `444` reaches `DIN 444 FORM B`       |
| `~6921`           | strip leading `~`              | "similar to DIN 6921"                     |

`tavor_std_code_variants()` is shared in spirit between both scripts.

### Standard equivalencies

`king/king_filter.py` has a `STANDARD_EQUIVALENCES` map for cross-family
aliases:
```python
("DIN", "7991"): ("ISO", "10642"),  # countersunk hex socket screws
```
Add new pairs as `(tavor_std_name, tavor_leading_numeric) →
(king_std_name, king_leading_numeric)`. Suffixes (FT, TX, F) carry across
automatically.

### King's fine-pitch handling

King keeps fine-pitch nuts in entries like `DIN 439 FP` with `SIZES = '8X1'`
(thread × pitch, not thread × length). `build_king_indexes` parses these
separately into `fine_nut` keyed by `(std_name, base_code, material, thread,
pitch)`. When a tavor row has a non-empty `Thread pitch`, lookup goes there
only — falling through to the coarse index would wrong-match the coarse SKU.

### King owners file (conflict log)

`Untitled spreadsheet - Аркуш1.csv` (filename hardcoded in `OWNERS_FILE`) is
a tavor import log: each line says `Штрихкод "X" вже присвоєно "Y …"`.
`load_owner_map` parses it into `{king_code: article_code}` and `dedupe_matches`
uses it to override `material_rank` — owner-designated rows always win their
king code. Currently honors 1,478 of 1,482 mappings (the rest are cases
where one article owns multiple king codes, which can't fit the
one-column-per-tavor-row schema).

## Eurobolt parser specifics

- `_STANDARD_RE` is anchored to family words (`DIN|ISO|UNI|ART|EN`) and a
  short uppercase suffix. The suffix needs `\b` so it doesn't swallow the
  size `M10x100` (M followed by digits is not a word-break).
- `_SIZE_XX_RE` first; `_THREAD_ONLY_RE` only as fallback. For nuts
  (`Гайка`/`Контргайка`), two-part `M10x1,25` is parsed as thread × pitch;
  for bolts/screws it's thread × length.
- `_CLASS_RE` matches bolt classes (4.6, 8.8, 10.9, ...) AND nut classes
  (4.0, 8.0, 10.0, ...). Strip the trailing `.0` so `10.0` becomes `10`,
  matching tavor's convention.
- `_STAINLESS_RE` handles `A2`/`A2-70`/`A4-80`. `_HV_RE` handles `200 HV`.
- Coating map covers Ukrainian abbreviations (`ЦБ→Zn`, `ЦЖ→YZn`, `БП→""`,
  `ГЦ→TZn`, `flZn480h→flZn`). The mapping is in `COATING_MAP`.

## Things that look like bugs but aren't

- Tavor `A4-80` matched against king `A4-80` (not `A4`) — king does have
  separate `A4` and `A4-80` materials, and the exact match wins via
  variant ordering. My old audit script that compared `base(mat) == km_mat`
  flagged 707 "mismatches" — false positives.
- King code `2093436` resolves to tavor article `001111` (A2-70), not
  `252337` (A2-50), even though A2-50 is the "standard" hex nut grade.
  The owners file overrides industry intuition with whatever tavor's
  import already accepted.
- ~14k unmatched stainless tavor rows are mostly `TN`-standard items
  (tavor-internal, 3,888 rows) and dimensions king doesn't stock
  (DIN 933 M5×85, etc.). Not a script bug.

## Running

```bash
.venv/bin/python king/king_filter.py
.venv/bin/python eurobolt/eurobolt_filter.py
```

Both scripts read from / write to the repo root. No CLI args. To change
inputs/outputs, edit the constants at the top of each script.

Typical run time: ~2 seconds king, ~3 seconds eurobolt.
