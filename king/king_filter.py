#!/usr/bin/env python3
"""Match rows in tavor.csv against king_data.csv and write tavor_with_king.csv
with two new columns: 'King Name' (DESCRIPTION) and 'King Code' (KING CODE).

King catalogue is stainless-steel only (A1/A2/A4/A5/AISI 304/316/420), so
tavor rows are considered candidates only when their Coating column is empty.

Tavor and king encode standard variants differently. The matcher normalizes
both sides:

  - tavor 912FT, 7991FT, 603FT, 14579FT  -> king 912, 7991, 603, 14579
    (FT = fully threaded; king does not split that out).
  - tavor 7380-1 / 7380-2                -> king 7380 / 7380 F.
  - tavor 7981C / 7982C / 14586C ...     -> king 7981 / 7982 / 14586
    (C = cross recess; king's no-suffix entry is the cross-recess form).
  - tavor 7981F                          -> king 7981 F.
  - tavor A2-70 / A4-80 / ...            -> king A2 / A4 (length grade).

Items without a length (nuts, washers, circlips) are matched against a
separate thread-only index built from king rows whose SIZES column has no
'X' separator.
"""
import re
import sys
import time

import pandas as pd

KING_FILE = "king_data.csv"
TAVOR_FILE = "tavor.csv"
OUT_FILE = "tavor_with_king.xlsx"
UNMATCHED_FILE = "king_unmatched.xlsx"

# Optional sidecar listing king_code -> article_code ownership lines copied
# from a tavor import-conflict log ('Штрихкод "X" вже присвоєно "Y NAME"').
# When present, those assignments override the material_rank heuristic so the
# dedup matches whatever tavor already considers canonical.
OWNERS_FILE = "Untitled spreadsheet - Аркуш1.csv"

# (tavor_std_name, leading_numeric) -> (king_std_name, leading_numeric).
# Used when tavor and king call the same fastener by different standards.
STANDARD_EQUIVALENCES = {
    ("DIN", "7991"): ("ISO", "10642"),  # countersunk hex socket screws
}


def normalize_num(val):
    """Return a canonical numeric string: '1.2', '3', None for blanks."""
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


def parse_king_size(size):
    """'1,2X3' -> ('1.2', '3'); '4X80' -> ('4', '80'); '1,5' -> ('1.5', None)."""
    if not size:
        return None, None
    s = str(size).strip()
    parts = re.split(r"[xXхХ*]", s, maxsplit=1)
    if len(parts) == 2:
        return normalize_num(parts[0]), normalize_num(parts[1])
    return normalize_num(parts[0]), None


def parse_king_standard(din_field):
    """'DIN 84' -> ('DIN', '84'); 'ISO 7380 P' -> ('ISO', '7380 P')."""
    if not din_field:
        return None, None
    s = str(din_field).strip()
    m = re.match(r"^\s*(\S+)\s+(.+)$", s)
    if not m:
        return s.upper(), None
    return m.group(1).upper(), m.group(2).strip()


def tavor_std_code_variants(code):
    """Yield candidate king std codes for a given tavor Standard Code."""
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return
    code = str(code).strip().lstrip("~").strip()  # '~6921' = 'similar to 6921'
    if not code:
        return
    seen = set()

    def emit(c):
        c = c.strip()
        if c and c not in seen:
            seen.add(c)
            return True
        return False

    if emit(code):
        yield code

    # FT (fully threaded) — king does not separate.
    if code.endswith("FT"):
        c = code[:-2]
        if emit(c):
            yield c

    # ISO 7380-1 / 7380-2 -> 7380 / 7380 F
    if code.endswith("-1"):
        c = code[:-2]
        if emit(c):
            yield c
    if code.endswith("-2"):
        c = code[:-2] + " F"
        if emit(c):
            yield c

    # Trailing single-letter suffix on a numeric base. Try the spaced form
    # first (DIN 6319 C, DIN 7981 F) — matches king's variant entries when
    # present. Then fall through to the bare number, which lets us match:
    #   - king's no-suffix default (tavor 7981C -> DIN 7981, since C means
    #     cross recess and king's plain entry is the cross-recess form);
    #   - king's loose index (tavor 444B -> DIN 444 FORM B via prefix '444');
    #   - king entries where tavor adds a letter king does not (DIN 125 A/B).
    m = re.match(r"^(\d+)([A-Z])$", code)
    if m:
        base, suf = m.group(1), m.group(2)
        spaced = f"{base} {suf}"
        if emit(spaced):
            yield spaced
        if emit(base):
            yield base


def tavor_standard_variants(std_name, std_code):
    """Yield (std_name, std_code) candidates for king lookup.

    Always emits all code variants under the original std_name first, then
    any alias defined in STANDARD_EQUIVALENCES (e.g. tavor DIN 7991 ->
    king ISO 10642). Aliases preserve any suffix (FT, TX, ...) and reuse the
    same code-variant rules.
    """
    if std_code is None or (isinstance(std_code, float) and pd.isna(std_code)):
        return
    code_str = str(std_code).strip()
    if not code_str:
        return

    seen = set()

    def emit(name, code):
        key = (name, code)
        if key in seen:
            return False
        seen.add(key)
        return True

    for code in tavor_std_code_variants(code_str):
        if emit(std_name, code):
            yield (std_name, code)

    leading = re.match(r"^(\d+)", code_str)
    if not leading:
        return
    alias = STANDARD_EQUIVALENCES.get((std_name, leading.group(1)))
    if not alias:
        return
    new_name, new_base = alias
    new_code = new_base + code_str[len(leading.group(1)):]
    for code in tavor_std_code_variants(new_code):
        if emit(new_name, code):
            yield (new_name, code)


def tavor_material_variants(material):
    """Yield candidate king MATERIAL values for a given tavor Material."""
    if not material:
        return
    m = str(material).strip().upper()
    if not m or m == "NAN":
        return
    seen = set()
    for cand in (
        m,
        re.sub(r"^(A[0-9])-\d+$", r"\1", m),  # A2-70 -> A2
        re.sub(r"^AISI\s*(\d+)$", r"AISI \1", m),  # AISI304 -> AISI 304
    ):
        if cand and cand not in seen:
            seen.add(cand)
            yield cand


_FP_RE = re.compile(r"\bFP\b")


def _strip_fp(std_code):
    """'439 FP' -> '439'; '934 FP LH' -> '934 LH'; otherwise None."""
    if not _FP_RE.search(std_code):
        return None
    stripped = _FP_RE.sub("", std_code)
    return re.sub(r"\s+", " ", stripped).strip()


def build_king_indexes(king_df):
    """Return (sized, nut, sized_loose, nut_loose, fine_nut).

    Strict indexes are keyed by king's std_code as parsed.

    Loose indexes are keyed by the leading numeric token of std_code (so
    'ISO 4027 EX DIN 914' is reachable as just '4027'). A loose key is only
    registered when:
      - std_code has a non-numeric suffix (otherwise it's already strict), and
      - that std_name has exactly ONE variant under that leading numeric.

    The single-variant guard prevents 'DIN 7981 F' / 'DIN 7981 TX' / ... from
    silently aliasing to one another via the bare prefix '7981'.

    fine_nut covers king's FP entries (fine pitch nuts: DIN 439 FP, DIN 934 FP,
    DIN 985 FP, ...). Their SIZES column is 'thread X pitch' rather than
    'thread X length', so it is parsed differently and indexed under the FP-
    stripped base code so tavor's plain '439' / '934' can find it.
    """
    rows = []
    fine_rows = []
    for _, row in king_df.iterrows():
        std_name, std_code = parse_king_standard(row.get("DIN"))
        if not std_name or not std_code:
            continue
        material = str(row.get("MATERIAL", "")).strip().upper()
        if not material:
            continue
        hit = (
            str(row.get("KING CODE", "")).strip(),
            str(row.get("DESCRIPTION", "")).strip(),
            str(row.get("SIZES", "")).strip(),
        )
        base_fp = _strip_fp(std_code)
        if base_fp is not None:
            # FP nut: SIZES is 'thread X pitch'.
            thread, pitch = parse_king_size(row.get("SIZES"))
            if thread and pitch:
                fine_rows.append((std_name, base_fp, material, thread, pitch, hit))
            continue
        thread, length = parse_king_size(row.get("SIZES"))
        if not thread:
            continue
        rows.append((std_name, std_code, material, thread, length, hit))

    variants_per_prefix = {}
    for std_name, std_code, *_ in rows:
        m = re.match(r"^(\d+)", std_code)
        if not m:
            continue
        prefix = m.group(1)
        variants_per_prefix.setdefault((std_name, prefix), set()).add(std_code)

    sized, nut, sized_loose, nut_loose = {}, {}, {}, {}
    for std_name, std_code, material, thread, length, hit in rows:
        if length:
            sized.setdefault((std_name, std_code, material, thread, length), hit)
        else:
            nut.setdefault((std_name, std_code, material, thread), hit)

        m = re.match(r"^(\d+)", std_code)
        if not m:
            continue
        prefix = m.group(1)
        if prefix == std_code:
            continue
        if len(variants_per_prefix[(std_name, prefix)]) != 1:
            continue
        if length:
            sized_loose.setdefault((std_name, prefix, material, thread, length), hit)
        else:
            nut_loose.setdefault((std_name, prefix, material, thread), hit)

    fine_nut = {}
    for std_name, base_code, material, thread, pitch, hit in fine_rows:
        fine_nut.setdefault((std_name, base_code, material, thread, pitch), hit)

    return sized, nut, sized_loose, nut_loose, fine_nut


def is_uncoated(coating):
    if coating is None:
        return True
    s = str(coating).strip().lower()
    return s in ("", "nan")


_DIAMETER_RE = re.compile(
    r"(?:ST|M|М)\s*(\d+(?:[.,]\d+)?)\s*[xXхХ*]\s*\d+", re.IGNORECASE
)


def diameter_from_name(name):
    """Pull a diameter (string) out of an Item Full Name like
    'Саморіз ST3,5x9,5 A2 ...' or 'Болт М12х1,5х130 ...'. Returns None on miss.
    """
    if not name:
        return None
    m = _DIAMETER_RE.search(str(name))
    if not m:
        return None
    return normalize_num(m.group(1))


def lookup(row, sized, nut, sized_loose, nut_loose, fine_nut):
    if not is_uncoated(row.get("Coating")):
        return None

    std_name = str(row.get("Standard Name", "")).strip().upper()
    if not std_name:
        return None
    thread = normalize_num(row.get("Metric thread M"))
    length = normalize_num(row.get("Length"))
    pitch = normalize_num(row.get("Thread pitch"))
    if not thread:
        thread = diameter_from_name(row.get("Item Full Name"))
    if not thread:
        return None

    std_variants = list(
        tavor_standard_variants(std_name, row.get("Standard Code", ""))
    )
    mat_variants = list(tavor_material_variants(row.get("Material", "")))
    if not std_variants or not mat_variants:
        return None

    # Fine pitch: tavor's Thread pitch is populated only for fine-pitch items.
    # King keeps those under a separate FP entry. Route there exclusively;
    # falling through to the coarse index would wrong-match the coarse SKU.
    if pitch:
        if length:
            # Fine-pitch bolt — king doesn't catalogue any in this dataset.
            return None
        for name, code in std_variants:
            for mat in mat_variants:
                hit = fine_nut.get((name, code, mat, thread, pitch))
                if hit:
                    return hit
        return None

    primary, fallback = (sized, sized_loose) if length else (nut, nut_loose)

    def _key(name, code, mat):
        return (
            (name, code, mat, thread, length) if length
            else (name, code, mat, thread)
        )

    for name, code in std_variants:
        for mat in mat_variants:
            hit = primary.get(_key(name, code, mat))
            if hit:
                return hit
    for name, code in std_variants:
        for mat in mat_variants:
            hit = fallback.get(_key(name, code, mat))
            if hit:
                return hit
    return None


def material_rank(item_article, material):
    """Lower is better. Used to break ties when multiple tavor rows hit the
    same king code.

    Tavor's 'A2-70' / 'A4-70' / 'A4-80' are the strength grades king implicitly
    stocks under the bare 'A2' / 'A4' label, so they are preferred over the
    less common grades (A2-50, A2-035, ...). Plain 'A2' / 'A4' wins outright.
    """
    mat = (material or "").upper().strip()
    if not mat:
        return 99
    if re.match(r"^(A[0-9]|AISI\s*\d+)$", mat):
        return 0
    for i, suf in enumerate(("-70", "-80", "-50", "-035", "-040")):
        if mat.endswith(suf):
            return 1 + i
    return 50


_OWNER_LINE_RE = re.compile(r'"([^"]+)"\s+вже присвоєно\s+"(\d+)\s')


def load_owner_map(path):
    """Parse a conflict log into {king_code: article_code}. Returns {} if the
    file is absent or unreadable."""
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except (FileNotFoundError, OSError):
        return {}
    owners = {}
    for col in df.columns:
        for cell in df[col]:
            m = _OWNER_LINE_RE.search(str(cell))
            if m:
                owners.setdefault(m.group(1), m.group(2))
    return owners


def dedupe_matches(tavor_df, owners):
    """Drop matches so each king code maps to at most one tavor row.

    If `owners[king_code]` exists, the row whose Article code matches wins.
    Otherwise fall back to material_rank.

    Returns the count of rows whose King columns were cleared.
    """
    mask = tavor_df["King Code"].astype(str) != ""
    if not mask.any():
        return 0
    matched = tavor_df.loc[mask].copy()
    matched["_rank"] = [
        material_rank(a, m)
        for a, m in zip(matched["Item article"], matched["Material"])
    ]
    # Boost owner-designated rows above any heuristic winner.
    matched["_owner_rank"] = [
        0 if owners.get(kc) == ac else 1
        for kc, ac in zip(matched["King Code"], matched["Article code"])
    ]
    matched = matched.sort_values(
        ["King Code", "_owner_rank", "_rank"], kind="stable"
    )
    winners = matched.drop_duplicates("King Code", keep="first").index
    losers = matched.index.difference(winners)
    if len(losers):
        tavor_df.loc[losers, ["King Name", "King Size", "King Code"]] = ""
    return len(losers)


def main():
    print(f"Loading {KING_FILE} and {TAVOR_FILE}...")
    king_df = pd.read_csv(
        KING_FILE, sep=";", dtype=str, keep_default_na=False, encoding="latin-1"
    )
    king_df.columns = [c.strip() for c in king_df.columns]
    tavor_df = pd.read_csv(TAVOR_FILE, dtype=str, on_bad_lines="skip")

    print(f"  king rows: {len(king_df)}")
    print(f"  tavor rows: {len(tavor_df)}")

    print("Building king indexes...")
    sized, nut, sized_loose, nut_loose, fine_nut = build_king_indexes(king_df)
    print(f"  sized keys:        {len(sized)} strict / {len(sized_loose)} loose")
    print(f"  thread-only keys:  {len(nut)} strict / {len(nut_loose)} loose")
    print(f"  fine-pitch keys:   {len(fine_nut)}")

    king_codes = []
    king_names = []
    king_sizes = []
    matched = 0
    matched_no_length = 0
    total = len(tavor_df)
    start = time.time()

    print("Matching tavor rows...")
    for idx, row in tavor_df.iterrows():
        hit = lookup(row, sized, nut, sized_loose, nut_loose, fine_nut)
        if hit:
            king_codes.append(hit[0])
            king_names.append(hit[1])
            king_sizes.append(hit[2])
            matched += 1
            if not normalize_num(row.get("Length")):
                matched_no_length += 1
        else:
            king_codes.append("")
            king_names.append("")
            king_sizes.append("")

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

    tavor_df["King Name"] = king_names
    tavor_df["King Size"] = king_sizes
    tavor_df["King Code"] = king_codes

    owners = load_owner_map(OWNERS_FILE)
    if owners:
        print(f"Loaded {len(owners)} owner assignments from {OWNERS_FILE!r}.")
    dropped = dedupe_matches(tavor_df, owners)
    matched -= dropped
    print(f"Enforcing one-to-one: cleared {dropped} duplicate matches.")

    print(f"Writing {OUT_FILE}...")
    with pd.ExcelWriter(OUT_FILE, engine="xlsxwriter") as writer:
        tavor_df.to_excel(writer, index=False, sheet_name="Sheet1")
        sheet = writer.sheets["Sheet1"]
        # Tavor article codes are zero-padded 6 digits ('039933'). Preserve
        # that visually with a number format on column A.
        article_fmt = writer.book.add_format({"num_format": "000000"})
        sheet.set_column("A:A", 10, article_fmt)
        # Widen the King columns so descriptions are readable at a glance.
        cols = list(tavor_df.columns)
        for label, width in (("King Name", 50), ("King Size", 10), ("King Code", 14)):
            if label in cols:
                idx = cols.index(label)
                sheet.set_column(idx, idx, width)

    matched_king_codes = {c for c in king_codes if c}
    unmatched_king = king_df[~king_df["KING CODE"].str.strip().isin(matched_king_codes)]

    print(f"Writing {UNMATCHED_FILE}...")
    with pd.ExcelWriter(UNMATCHED_FILE, engine="xlsxwriter") as writer:
        unmatched_king.to_excel(writer, index=False, sheet_name="Sheet1")
        sheet = writer.sheets["Sheet1"]
        widths = {
            "CUSTOMER CODE": 14,
            "MATERIAL": 10,
            "DIN": 18,
            "DESCRIPTION": 50,
            "SIZES": 12,
            "KING CODE": 16,
            "OUR CODE": 12,
        }
        for col, width in widths.items():
            if col in unmatched_king.columns:
                idx = list(unmatched_king.columns).index(col)
                sheet.set_column(idx, idx, width)

    print(
        f"Done. Matched {matched} / {total} tavor rows ({matched / total:.1%}); "
        f"{matched_no_length} of those are nuts/washers (no length). "
        f"{len(unmatched_king)} / {len(king_df)} king rows unmatched."
    )


if __name__ == "__main__":
    main()
