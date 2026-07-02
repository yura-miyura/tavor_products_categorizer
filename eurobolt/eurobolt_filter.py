#!/usr/bin/env python3
"""Match rows in tavor.csv against eurobolt.xlsx and write
tavor_with_eurobolt.xlsx with two new columns: 'Eurobolt Name' (full
nomenclature) and 'Eurobolt Article' (EU-XXXXXXXX code). Also writes
eurobolt_unmatched.xlsx listing eurobolt rows tavor doesn't carry.

Unlike king_data.csv (structured columns), eurobolt is a price list whose
'Номенклатура' field encodes everything in free text, e.g.:
    'Болт DIN 933 М10х100 8,8 ЦБ'
    'Гвинт пкр. гол. внут. шест. ISO 7380 М10х20 А2'
    'Гайка DIN 934 М10 10,0 ЦБ'

So the matcher first parses each eurobolt name into structured fields
(standard, thread, length, pitch, material, coating), builds an index, and
then for each tavor row looks up the corresponding eurobolt entry. Coatings
and materials are normalized to tavor's vocabulary (ЦБ -> Zn, ЦЖ -> YZn,
А2-70 -> A2-70, 8,8 -> 8.8, etc.).
"""
import re
import sys
import time
import zipfile
import os
import tempfile

import pandas as pd

EUROBOLT_FILE = "eurobolt.xlsx"
TAVOR_FILE = "tavor.csv"
OUT_FILE = "tavor_with_eurobolt.xlsx"
UNMATCHED_FILE = "eurobolt_unmatched.xlsx"


# Cyrillic letters that look identical to Latin: М (U+041C), А (U+0410), х (U+0445).
_LAT_FROM_CYR = str.maketrans({"М": "M", "А": "A", "В": "B", "С": "C", "Е": "E",
                                "Н": "H", "К": "K", "Р": "P", "Т": "T", "Х": "X",
                                "О": "O", "м": "m", "а": "a", "в": "b", "с": "c",
                                "е": "e", "н": "h", "к": "k", "р": "p", "т": "t",
                                "х": "x", "о": "o"})


def latinize(text):
    return (text or "").translate(_LAT_FROM_CYR)


def normalize_num(val):
    if val is None:
        return None
    s = str(val).strip().replace(",", ".")
    if not s or s.lower() == "nan":
        return None
    try:
        f = float(s)
    except ValueError:
        return s
    if f == int(f):
        return str(int(f))
    return ("%g" % f)


# eurobolt coating tokens -> tavor canonical coating
COATING_MAP = {
    "ЦБ": "Zn",
    "ЦЖ": "YZn",
    "ЦЧ": "BZn",
    "БП": "",
    "ГЦ": "TZn",
    "ЦХР": "Cr",
    "FLZN480H": "flZn",
    "FLZN": "flZn",
    "FLZNNC": "flZn",
    "ОБМІД.": "Cu",
    "ОБМ.": "Cu",
}


def normalize_coating(token):
    """Return tavor-canonical coating for an eurobolt token, or None if the
    token isn't a recognized coating."""
    if not token:
        return None
    key = token.strip().upper()
    return COATING_MAP.get(key)


# property classes / hardness / stainless grades the parser recognizes
_CLASS_RE = re.compile(
    r"\b(4[.,]6|4[.,]8|5[.,]6|5[.,]8|6[.,]8|8[.,]8|10[.,]9|12[.,]9|"
    r"4[.,]0|5[.,]0|6[.,]0|8[.,]0|10[.,]0|12[.,]0)\b"
)
_HV_RE = re.compile(r"\b(\d+)\s*HV\b", re.IGNORECASE)
_STAINLESS_RE = re.compile(r"\bA([1-5])(?:-(\d+))?\b")
_STANDARD_RE = re.compile(
    # Standard family + numeric base, optionally followed by a short
    # uppercase suffix as a separate word ('7380 F', '7505 A'). The
    # word boundary after the suffix prevents the size 'M10x100' from
    # being absorbed (M is followed by digits, not a word break).
    r"\b(DIN|ISO|UNI|ART\.?|EN)\s*"
    r"(\d+(?:[-/]\d+)?(?:\s+[A-Z]{1,3}\b)?)",
    re.IGNORECASE,
)
# Size with explicit pitch/length: 'M10x100', 'M10x1,25x60', '2,9x13'.
_SIZE_XX_RE = re.compile(
    r"(?:\bM\s*)?(\d+(?:[.,]\d+)?)\s*[x*]\s*"
    r"(?:(\d+(?:[.,]\d+)?)\s*[x*]\s*)?"
    r"(\d+(?:[.,]\d+)?)",
    re.IGNORECASE,
)
_THREAD_ONLY_RE = re.compile(r"\bM\s*(\d+(?:[.,]\d+)?)\b", re.IGNORECASE)
# trailing pack size like "(100)" or "(50)"
_PACK_RE = re.compile(r"\(\s*\d[\d\s]*\)\s*$")


def parse_eurobolt_name(name):
    """Extract structured fields from a free-text eurobolt nomenclature.

    Returns a dict with std_name, std_code, thread, length, pitch, material,
    coating. Missing fields are None / "" (coating).
    """
    if not name:
        return {}
    raw = _PACK_RE.sub("", name).strip()
    lat = latinize(raw)

    out = {"std_name": None, "std_code": None, "thread": None, "length": None,
           "pitch": None, "material": "", "coating": ""}

    m = _STANDARD_RE.search(lat)
    if m:
        out["std_name"] = m.group(1).upper().rstrip(".")
        out["std_code"] = re.sub(r"\s+", " ", m.group(2)).strip().upper()
        # strip the standard from the working string so size patterns don't
        # mis-grab the standard number.
        lat = lat[:m.start()] + " " + lat[m.end():]

    # Prefer the explicit DxD(xD) size; otherwise fall back to bare 'M<d>'.
    # For nuts ('Гайка', 'Контргайка'), a 2-part 'M10x1,25' means
    # thread x pitch (no length); for bolts/screws it means thread x length.
    is_nut = any(w in (name or "") for w in ("Гайка", "Контргайка"))
    size = _SIZE_XX_RE.search(lat)
    if size:
        a = normalize_num(size.group(1))
        b = normalize_num(size.group(2)) if size.group(2) else None
        c = normalize_num(size.group(3))
        if b:
            out["thread"], out["pitch"], out["length"] = a, b, c
        elif is_nut:
            out["thread"], out["pitch"] = a, c
        else:
            out["thread"], out["length"] = a, c
    else:
        m2 = _THREAD_ONLY_RE.search(lat)
        if m2:
            out["thread"] = normalize_num(m2.group(1))

    # material: try property class, then stainless, then HV hardness
    m = _CLASS_RE.search(lat)
    if m:
        out["material"] = m.group(1).replace(",", ".")
        # nut classes like '10,0' / '8,0' are stored in tavor as '10' / '8'
        if out["material"].endswith(".0"):
            out["material"] = out["material"][:-2]
    else:
        m = _STAINLESS_RE.search(lat)
        if m:
            grade = m.group(2)
            out["material"] = f"A{m.group(1)}" + (f"-{grade}" if grade else "")
        else:
            m = _HV_RE.search(lat)
            if m:
                out["material"] = f"{m.group(1)}HV"

    # coating: scan tokens, last hit wins
    for tok in lat.split():
        c = normalize_coating(tok)
        if c is not None:
            out["coating"] = c
    return out


def _s(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def fastener_key(std_name, std_code, material, coating, thread, length, pitch):
    """Canonical lookup key. Both sides must agree on each field for a match."""
    return (
        _s(std_name).upper(),
        _s(std_code).upper(),
        _s(material).upper(),
        _s(coating),
        _s(thread),
        _s(length),
        _s(pitch),
    )


def tavor_std_code_variants(code):
    """Yield king-style variant codes for a tavor Standard Code. Eurobolt
    catalogue uses the canonical/shorter form, while tavor often glues a
    recess/form suffix on (7981C, 7380-1, 912FT). The variants mirror those
    used in king/king_filter.py."""
    code = _s(code).lstrip("~").strip()
    if not code:
        return
    seen = set()

    def emit(c):
        if c and c not in seen:
            seen.add(c)
            return True
        return False

    if emit(code):
        yield code
    if code.endswith("FT") and emit(code[:-2]):  # 912FT -> 912
        yield code[:-2]
    if code.endswith("-1") and emit(code[:-2]):  # 7380-1 -> 7380
        yield code[:-2]
    if code.endswith("-2"):                       # 7380-2 -> 7380 F
        c = code[:-2] + " F"
        if emit(c):
            yield c
    m = re.match(r"^(\d+)([A-Z])$", code)         # 7981C, 444B, 125A...
    if m:
        spaced = f"{m.group(1)} {m.group(2)}"
        if emit(spaced):
            yield spaced
        if emit(m.group(1)):
            yield m.group(1)


def tavor_material_variants(material):
    """Yield candidate eurobolt MATERIAL values for a tavor Material."""
    m = _s(material).upper()
    if not m:
        return
    yield m
    base = re.match(r"^(A[0-9])-\d+$", m)         # A2-70 -> A2
    if base:
        yield base.group(1)


_DIAMETER_RE = re.compile(
    r"(?:ST|M|М)\s*(\d+(?:[.,]\d+)?)\s*[xXхХ*]\s*\d+", re.IGNORECASE
)


def diameter_from_name(name):
    """Recover a missing thread/diameter from a tavor Item Full Name. Same
    fallback as king/king_filter.py to handle self-tappers ('ST2,2x4,5')
    whose Metric thread M column is left blank."""
    if not name:
        return None
    m = _DIAMETER_RE.search(str(name))
    if not m:
        return None
    return normalize_num(m.group(1))


def tavor_lookup_keys(row):
    """Yield every fastener_key tavor row could plausibly be filed under."""
    std_name = _s(row.get("Standard Name")).upper()
    if not std_name:
        return
    thread = normalize_num(row.get("Metric thread M"))
    if not thread:
        thread = diameter_from_name(row.get("Item Full Name"))
    if not thread:
        return
    length = normalize_num(row.get("Length")) or ""
    pitch = normalize_num(row.get("Thread pitch")) or ""
    coating = _s(row.get("Coating"))
    code_variants = list(tavor_std_code_variants(row.get("Standard Code")))
    mat_variants = list(tavor_material_variants(row.get("Material")))
    if not code_variants:
        return
    if not mat_variants:
        mat_variants = [""]
    for code in code_variants:
        for mat in mat_variants:
            yield fastener_key(
                std_name, code, mat, coating, thread, length, pitch
            )


def _open_xlsx_with_case_fix(path):
    """eurobolt.xlsx was produced with 'xl/SharedStrings.xml' instead of the
    lowercase name openpyxl expects. If reading fails for that reason,
    rewrite a temp copy with the corrected name."""
    try:
        return pd.ExcelFile(path)
    except KeyError as e:
        if "sharedStrings.xml" not in str(e):
            raise
        tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        tmp.close()
        try:
            with zipfile.ZipFile(path) as zin, zipfile.ZipFile(
                tmp.name, "w", zipfile.ZIP_DEFLATED
            ) as zout:
                for item in zin.infolist():
                    name = item.filename
                    if name == "xl/SharedStrings.xml":
                        name = "xl/sharedStrings.xml"
                    zout.writestr(name, zin.read(item.filename))
            return pd.ExcelFile(tmp.name)
        except Exception:
            os.unlink(tmp.name)
            raise


def load_eurobolt(path):
    """Return a DataFrame with columns: name, article, parsed fields."""
    xls = _open_xlsx_with_case_fix(path)
    raw = pd.read_excel(xls, sheet_name=xls.sheet_names[0], dtype=str,
                        keep_default_na=False)
    # In the source the article column is the one filled with EU-XXXXXXXX.
    article_col = None
    for c in raw.columns:
        if raw[c].astype(str).str.startswith("EU-").any():
            article_col = c
            break
    if article_col is None:
        raise ValueError("Could not find article column (no EU- prefixed cells)")
    # The name column is the long-text one with the most non-empty values.
    candidates = [
        c for c in raw.columns
        if c != article_col and raw[c].astype(str).str.len().mean() > 10
    ]
    name_col = max(candidates, key=lambda c: raw[c].astype(str).str.len().sum())

    df = pd.DataFrame({
        "name": raw[name_col].astype(str).str.strip(),
        "article": raw[article_col].astype(str).str.strip(),
    })
    df = df[df["article"].str.startswith("EU-")].reset_index(drop=True)
    parsed = df["name"].apply(parse_eurobolt_name)
    for k in ("std_name", "std_code", "thread", "length", "pitch",
              "material", "coating"):
        df[k] = [p.get(k) if p.get(k) is not None else "" for p in parsed]
    return df


def _eurobolt_material_variants(material):
    """Yield material values to register an eurobolt row under. 'A2-70' is
    also indexed as bare 'A2' so a tavor row with plain Material='A2' can
    find it."""
    m = _s(material).upper()
    if not m:
        yield ""
        return
    yield m
    base = re.match(r"^(A[0-9])-\d+$", m)
    if base:
        yield base.group(1)


def build_eurobolt_index(df):
    """Group eurobolt rows by fastener_key. Each row is registered under its
    parsed material AND its grade-stripped variant ('A2-70' → also 'A2'),
    with the more specific key inserted first so it wins on collision."""
    idx = {}
    for _, row in df.iterrows():
        if not row["std_name"] or not row["std_code"] or not row["thread"]:
            continue
        hit = (row["name"], row["article"])
        for mat in _eurobolt_material_variants(row["material"]):
            key = fastener_key(
                row["std_name"], row["std_code"], mat, row["coating"],
                row["thread"], row["length"], row["pitch"],
            )
            idx.setdefault(key, hit)
    return idx


def is_uncoated(coating):
    if coating is None:
        return True
    s = str(coating).strip().lower()
    return s in ("", "nan")


def lookup(row, index):
    """Look up an eurobolt entry matching this tavor row, trying every
    code/material variant."""
    for key in tavor_lookup_keys(row):
        hit = index.get(key)
        if hit:
            return hit
    return None


def material_rank(material):
    """Lower is better. Used to break ties when multiple tavor rows match
    the same eurobolt article. Mirrors king/king_filter.py."""
    mat = _s(material).upper()
    if not mat:
        return 99
    if re.match(r"^(A[0-9]|AISI\s*\d+)$", mat):
        return 0
    for i, suf in enumerate(("-70", "-80", "-50", "-035", "-040")):
        if mat.endswith(suf):
            return 1 + i
    return 50


def dedupe_matches(tavor_df):
    """Drop matches so each eurobolt article maps to at most one tavor row.
    Returns the count of cleared duplicates."""
    mask = tavor_df["Eurobolt Article"].astype(str) != ""
    if not mask.any():
        return 0
    matched = tavor_df.loc[mask].copy()
    matched["_rank"] = [material_rank(m) for m in matched["Material"]]
    matched = matched.sort_values(
        ["Eurobolt Article", "_rank"], kind="stable"
    )
    winners = matched.drop_duplicates("Eurobolt Article", keep="first").index
    losers = matched.index.difference(winners)
    if len(losers):
        tavor_df.loc[losers, ["Eurobolt Name", "Eurobolt Article"]] = ""
    return len(losers)


def main():
    print(f"Loading {EUROBOLT_FILE} and {TAVOR_FILE}...")
    eu_df = load_eurobolt(EUROBOLT_FILE)
    tavor_df = pd.read_csv(TAVOR_FILE, dtype=str, on_bad_lines="skip")
    print(f"  eurobolt rows: {len(eu_df)}")
    print(f"  tavor rows:    {len(tavor_df)}")

    print("Building eurobolt index...")
    index = build_eurobolt_index(eu_df)
    print(f"  index keys: {len(index)}")

    names, articles = [], []
    matched = 0
    total = len(tavor_df)
    start = time.time()

    print("Matching tavor rows...")
    for idx, row in tavor_df.iterrows():
        hit = lookup(row, index)
        if hit:
            names.append(hit[0])
            articles.append(hit[1])
            matched += 1
        else:
            names.append("")
            articles.append("")
        if idx and idx % 5000 == 0:
            elapsed = time.time() - start
            rate = idx / elapsed if elapsed else 0
            eta = (total - idx) / rate if rate else 0
            mins, secs = divmod(int(eta), 60)
            sys.stdout.write(
                f"\r  {idx}/{total}  matched={matched}  ETA {mins:02d}:{secs:02d}"
            )
            sys.stdout.flush()
    print()

    tavor_df["Eurobolt Name"] = names
    tavor_df["Eurobolt Article"] = articles

    dropped = dedupe_matches(tavor_df)
    matched -= dropped
    print(f"Enforcing one-to-one: cleared {dropped} duplicate matches.")

    print(f"Writing {OUT_FILE}...")
    with pd.ExcelWriter(OUT_FILE, engine="xlsxwriter") as writer:
        tavor_df.to_excel(writer, index=False, sheet_name="Sheet1")
        sheet = writer.sheets["Sheet1"]
        article_fmt = writer.book.add_format({"num_format": "000000"})
        sheet.set_column("A:A", 10, article_fmt)
        cols = list(tavor_df.columns)
        for label, width in (("Eurobolt Name", 55), ("Eurobolt Article", 16)):
            if label in cols:
                i = cols.index(label)
                sheet.set_column(i, i, width)

    matched_articles = set(
        a for a in tavor_df["Eurobolt Article"].astype(str) if a
    )
    unmatched_eu = eu_df[~eu_df["article"].isin(matched_articles)][
        ["name", "article"]
    ]

    print(f"Writing {UNMATCHED_FILE}...")
    with pd.ExcelWriter(UNMATCHED_FILE, engine="xlsxwriter") as writer:
        unmatched_eu.to_excel(writer, index=False, sheet_name="Sheet1")
        sheet = writer.sheets["Sheet1"]
        sheet.set_column("A:A", 60)
        sheet.set_column("B:B", 16)

    print(
        f"Done. Matched {matched} / {total} tavor rows ({matched / total:.1%}). "
        f"{len(unmatched_eu)} / {len(eu_df)} eurobolt rows unmatched."
    )


if __name__ == "__main__":
    main()
