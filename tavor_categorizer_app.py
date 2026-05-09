#!/usr/bin/env python3
import pandas as pd
import re
import time
import sys

def parse_baza_coating(row):
    """
    Визначає код покриття для бази на основі офіційного списку відповідностей.
    """
    desc = str(row.get('Unnamed: 3', '')).lower()
    col_coating = str(row.get('Покриття', '')).lower()

    combined_text = f"{desc} {col_coating}"

    # 1. Цинк платковий чорний (BFZn)
    if any(term in combined_text for term in ['цинк платковий чорний', 'чорний платковий']):
        return 'BFZn'

    # 2. Цинк платковий (FZn)
    elif any(term in combined_text for term in ['цинк платковий', 'цпз', 'fzn']):
        return 'FZn'

    # 3. Цинк гарячий (TZn)
    elif any(term in combined_text for term in ['цинк гарячий', 'гарячий цинк', 'гц', 'tzn']):
        return 'TZn'

    # 4. Цинк, Нейлон / Нейлон, Цинк (Zn+Nylon)
    elif 'нейлон' in combined_text and ('цинк' in combined_text or 'zn' in combined_text):
        return 'Zn+Nylon'

    # 5. Цинк механічний (mYZn або mZn)
    elif any(term in combined_text for term in ['механічний', 'механічно']):
        if 'жовт' in combined_text:
            return 'mYZn'
        return 'mZn'

    # 6. Цинк жовтий (YZn)
    elif any(term in combined_text for term in ['цинк жовтий', 'жовтий цинк', 'жц', 'yzn']):
        return 'YZn'

    # 7. Цинк чорний (BZn)
    elif any(term in combined_text for term in ['цинк чорний', 'чорний цинк', 'bzn']):
        return 'BZn'

    # 8. Нікель (Ni Zn)
    elif any(term in combined_text for term in ['нікель', 'ni zn']):
        return 'Ni Zn'

    # 9. Фосфат (ph)
    elif any(term in combined_text for term in ['фосфат', 'ph']):
        return 'ph'

    # 10. Мідь (Cu) - Додано!
    elif any(term in combined_text for term in ['мідь', 'мід', 'cu']):
        return 'Cu'

    # 11. Звичайний Цинк (Zn)
    elif any(term in combined_text for term in ['цинк', ' ц ', 'ц.', 'zn', 'білий']):
        return 'Zn'

    # 12. Без покриття - Оновлено!
    elif any(term in combined_text for term in ['без покриття', 'чорний', 'blk']):
        return 'без покриття'

    # Якщо нічого не вказано (наприклад для нержавійки) - за замовчуванням "без покриття"
    return 'без покриття'


def parse_baza_material(row):
    """
    Витягує клас міцності або тип нержавійки (A2/A4).
    """
    desc = str(row.get('Unnamed: 3', '')) + " " + str(row.get('Клас міцності', ''))

    a2_a4_match = re.search(r'([AАaа][24])', desc)
    if a2_a4_match:
        val = a2_a4_match.group(1).upper()
        return val.replace('А', 'A')

    class_match = re.search(r'\b0?(4[.,]6|5[.,]6|5[.,]8|8[.,]8|10[.,]9|12[.,]9)\b', desc)
    if class_match:
        return class_match.group(1).replace(',', '.')

    return None


def parse_baza_dimensions(row):
    """
    Витягує Діаметр, Крок різьби (якщо є) та Довжину.
    """
    desc = str(row.get('Unnamed: 3', ''))

    pattern = r'(?:M|m|DIA|dia)?\s*(\d+(?:[.,]\d+)?)\s*[xX*хХ]\s*(?:(\d+(?:[.,]\d+)?)\s*[xX*хХ]\s*)?(\d+(?:[.,]\d+)?)'
    dim_match = re.search(pattern, desc)

    if dim_match:
        part1 = dim_match.group(1).replace(',', '.')
        part2 = dim_match.group(2).replace(',', '.') if dim_match.group(2) else None
        part3 = dim_match.group(3).replace(',', '.')

        thread = str(float(part1)).rstrip('0').rstrip('.')
        length = str(float(part3)).rstrip('0').rstrip('.')
        pitch = str(float(part2)).rstrip('0').rstrip('.') if part2 else None

        return thread, length, pitch

    try:
        t = str(row.get('Діаметр', '')).replace(',', '.').strip()
        l = str(row.get('Довжина', '')).replace(',', '.').strip()
        thread = str(float(t)).rstrip('0').rstrip('.') if t != "nan" and t else None
        length = str(float(l)).rstrip('0').rstrip('.') if l != "nan" and l else None
        return thread, length, None
    except:
        return None, None, None


def prep_tavor_data(tavor_df):
    """
    Готує дані Tavor для порівняння.
    """
    def clean_tavor_val(val):
        v = str(val).strip().replace(',', '.')
        if v.endswith('.0'):
            return v[:-2]
        return v if v != 'nan' else None

    tavor_df['T_Thread'] = tavor_df['Metric thread M'].apply(clean_tavor_val)
    tavor_df['T_Length'] = tavor_df['Length'].apply(clean_tavor_val)

    tavor_df['T_Std_Name'] = tavor_df['Standard Name'].astype(str).str.strip().str.upper()
    tavor_df['T_Std_Code'] = tavor_df['Standard Code'].astype(str).str.strip()
    tavor_df['T_Material'] = tavor_df['Material'].astype(str).str.strip()

    # ВАЖЛИВО: Замінили 'BLK' на 'без покриття' для порожніх полів Tavor
    tavor_df['T_Coating'] = tavor_df['Coating'].fillna('без покриття').replace({'nan': 'без покриття', '': 'без покриття'})

    return tavor_df


def match_row_to_tavor(row, tavor_df):
    """
    Шукає ідеальний збіг у Tavor.
    """
    full_desc = str(row.get('Unnamed: 3', '')) + " " + str(row.get('Unnamed: 2', ''))
    std_match = re.search(r'(DIN|ISO)\s*(\d+(?:[-/]\d+)?)', full_desc, re.IGNORECASE)

    std_name = std_match.group(1).upper() if std_match else None
    std_code = std_match.group(2) if std_match else None

    th, ln, pitch = parse_baza_dimensions(row)
    mat = parse_baza_material(row)
    coat = parse_baza_coating(row)

    if th and ln and std_name and std_code:
        th_with_pitch = f"{th}x{pitch}" if pitch else th
        mask_thread = (tavor_df['T_Thread'] == th) | (tavor_df['T_Thread'] == th_with_pitch)

        mask = (
            (tavor_df['T_Std_Name'] == std_name) &
            (tavor_df['T_Std_Code'] == std_code) &
            mask_thread &
            (tavor_df['T_Length'] == ln) &
            (tavor_df['T_Coating'] == coat)
        )

        if mat:
            if mat in ['A2', 'A4']:
                mask = mask & tavor_df['T_Material'].str.contains(mat, na=False)
            else:
                mask = mask & (tavor_df['T_Material'] == mat)

        if pitch:
            pitch_comma = pitch.replace('.', ',')
            mask_pitch = (
                (tavor_df['T_Thread'] == th_with_pitch) |
                tavor_df['Item Full Name'].str.contains(pitch, regex=False, na=False) |
                tavor_df['Item Full Name'].str.contains(pitch_comma, regex=False, na=False)
            )
            mask = mask & mask_pitch

        res = tavor_df[mask]
        if not res.empty:
            raw_id = str(res.iloc[0]['Article code']).split('.')[0]
            return raw_id.zfill(6), res.iloc[0]['Item Full Name']

    return None, None


def main():
    print("Завантаження файлів...")
    baza_df = pd.read_csv('baza.csv')
    tavor_df = pd.read_csv('TAVOR.csv', on_bad_lines='skip', dtype=str)

    tavor_df = prep_tavor_data(tavor_df)

    total_items = len(baza_df)
    start_time = time.time()
    final_rows = []

    print("\nПочаток зіставлення товарів...")
    for idx, row in baza_df.iterrows():
        tavor_id, tavor_about = match_row_to_tavor(row, tavor_df)

        row_dict = row.to_dict()
        row_dict['tavor_id'] = tavor_id if tavor_id else ""
        row_dict['tavor_description'] = tavor_about if tavor_about else ""
        final_rows.append(row_dict)

        if idx > 0 and idx % 50 == 0:
            elapsed = time.time() - start_time
            items_per_sec = idx / elapsed
            remaining_items = total_items - idx

            if items_per_sec > 0:
                est_time_left_sec = remaining_items / items_per_sec
                mins, secs = divmod(est_time_left_sec, 60)
                time_str = f"{int(mins):02d}:{int(secs):02d}"
            else:
                time_str = "00:00"

            sys.stdout.write(f"\rОбробка {idx}/{total_items} | Очікуваний час: {time_str}    ")
            sys.stdout.flush()

    print("\n\nФорматування фінального файлу...")
    result_df = pd.DataFrame(final_rows)

    cols = result_df.columns.tolist()
    if 'tavor_id' in cols and 'tavor_description' in cols:
        cols.insert(4, cols.pop(cols.index('tavor_id')))
        cols.insert(5, cols.pop(cols.index('tavor_description')))
    result_df = result_df[cols]

    result_df['tavor_id'] = pd.to_numeric(result_df['tavor_id'], errors='coerce')

    output_name = 'baza_updated.xlsx'
    print(f"Збереження в Excel '{output_name}'...")

    with pd.ExcelWriter(output_name, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, index=False)
        fmt = writer.book.add_format({'num_format': '000000'})
        writer.sheets['Sheet1'].set_column('E:E', 15, fmt)

    matched_count = result_df['tavor_id'].notna().sum()
    print(f"\nГОТОВО! Успішно знайдено {matched_count} з {total_items} товарів.")

if __name__ == "__main__":
    main()
